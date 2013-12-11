package sysinit

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/dotcloud/docker/netlink"
	"github.com/dotcloud/docker/utils"
	"github.com/guelfey/go.dbus"
	"github.com/guelfey/go.dbus/introspect"
	"github.com/kr/pty"
	"github.com/syndtr/gocapability/capability"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type DockerInitArgs struct {
	user       string
	gateway    string
	workDir    string
	ip         string
	privileged bool
	tty        bool
	openStdin  bool
	child      bool
	env        []string
	args       []string
}

const SharedPath = "/.docker-shared"
const DBusSocketName = "dbus.sock"
const DockerInitPath = "/com/docker/DockerInit"
const DockerInitInterface = "com.docker.DockerInit"

type State int32

const (
	Initial State = iota
	Running
	Exited
	FailedToStart
)

// client side wrapper of the com.docker.DockerInit dbus interface
type DockerInitWrapper struct {
	obj         *dbus.Object
	stateChange chan *dbus.Signal
	conn        *dbus.Conn
}

func DockerInitWrap(conn *dbus.Conn) *DockerInitWrapper {
	wrap := &DockerInitWrapper{
		obj:  conn.Object("", DockerInitPath),
		conn: conn,
	}

	wrap.stateChange = make(chan *dbus.Signal, 10)
	conn.Signal(wrap.stateChange)
	return wrap
}

func (wrap *DockerInitWrapper) Close() {
	closed := false

	// Its currently unsafe to close a connection multiple times (https://github.com/guelfey/go.dbus/issues/47)
	// We check for the signal channel being closed to see if it is safe to close
	eof := false
	for !eof {
		select {
		case _, ok := <-wrap.stateChange:
			if !ok {
				closed = true
				eof = true
			}
		default:
			eof = true
		}
	}
	if !closed {
		wrap.conn.Close()
	}
}

func (wrap *DockerInitWrapper) WaitForStateChange() (State, string, int) {
	s := <-wrap.stateChange

	// Dockerinit died and closed the connection. This happens either
	// if there is an internal error in .dockerinit, or in a machine-style
	// container where pid1 dies. We treat this as if the container exited
	// normally signalling error, as init dying is not expected in the
	// machine-style container case.
	if s == nil {
		return Exited, "", 1
	}

	state, _ := s.Body[0].(int32)
	errStr, _ := s.Body[1].(string)
	exitStatus, _ := s.Body[1].(int32)

	return State(state), errStr, int(exitStatus)
}

// Get the current state (started/running/exited
func (wrap *DockerInitWrapper) GetState() (State, error) {
	var res int32
	err := wrap.obj.Call(DockerInitInterface+".GetState", 0).Store(&res)
	return State(res), err
}

func (wrap *DockerInitWrapper) Resume() error {
	return wrap.obj.Call(DockerInitInterface+".Resume", 0).Store()
}

func (wrap *DockerInitWrapper) GetPtyMaster() (*os.File, error) {
	var fd dbus.UnixFD
	if err := wrap.obj.Call(DockerInitInterface+".GetPtyMaster", 0).Store(&fd); err != nil {
		return nil, err
	}

	// Hmm, shouldn't dbus do this?
	syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), syscall.F_SETFD, syscall.FD_CLOEXEC)

	return os.NewFile(uintptr(fd), "ptyMaster"), nil
}

func (wrap *DockerInitWrapper) GetStdOut() (*os.File, *os.File, error) {
	var stdoutFd, stderrFd dbus.UnixFD
	if err := wrap.obj.Call(DockerInitInterface+".GetStdOut", 0).Store(&stdoutFd, &stderrFd); err != nil {
		return nil, nil, err
	}

	// Hmm, shouldn't dbus do this?
	syscall.Syscall(syscall.SYS_FCNTL, uintptr(stdoutFd), syscall.F_SETFD, syscall.FD_CLOEXEC)
	syscall.Syscall(syscall.SYS_FCNTL, uintptr(stderrFd), syscall.F_SETFD, syscall.FD_CLOEXEC)

	return os.NewFile(uintptr(stdoutFd), "stdout"), os.NewFile(uintptr(stderrFd), "stderr"), nil
}

func (wrap *DockerInitWrapper) GetStdIn() (*os.File, error) {
	var fd dbus.UnixFD
	if err := wrap.obj.Call(DockerInitInterface+".GetStdIn", 0).Store(&fd); err != nil {
		return nil, err
	}

	// Hmm, shouldn't dbus do this?
	syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), syscall.F_SETFD, syscall.FD_CLOEXEC)

	return os.NewFile(uintptr(fd), "stdin"), nil
}

func (wrap *DockerInitWrapper) Signal(signal int) error {
	return wrap.obj.Call(DockerInitInterface+".Signal", 0, int32(signal)).Store()
}

func dbusSocketPath() string {
	return path.Join(SharedPath, DBusSocketName)
}

type DockerInit struct {
	sync.Mutex
	introspectable introspect.Introspectable
	state          State
	resume         chan int
	exitStatus     int32
	error          string
	process        *os.Process
	stdin          *os.File
	stdout         *os.File
	stderr         *os.File
	ptyMaster      *os.File
	openStdin      bool
	connections    []*dbus.Conn
}

func wrap(err error) *dbus.Error {
	return &dbus.Error{
		"com.docker.Error.Generic",
		[]interface{}{err.Error()},
	}
}

// Get the current state (started/running/exited
func (init *DockerInit) GetState() (int32, *dbus.Error) {
	init.Lock()
	defer init.Unlock()
	return int32(init.state), nil
}

// Get the exit code (or -1 if running)
func (init *DockerInit) GetExitStatus() (int32, *dbus.Error) {
	init.Lock()
	defer init.Unlock()
	return init.exitStatus, nil
}

func (init *DockerInit) Resume() *dbus.Error {
	init.Lock()
	defer init.Unlock()
	init.resume <- 1
	return nil
}

// Send a signal to the container app
func (init *DockerInit) Signal(signal int32) *dbus.Error {
	init.Lock()
	defer init.Unlock()
	if err := init.process.Signal(syscall.Signal(signal)); err != nil {
		return wrap(err)
	}
	return nil
}

func (init *DockerInit) GetPtyMaster() (dbus.UnixFD, *dbus.Error) {
	init.Lock()
	defer init.Unlock()

	if init.ptyMaster == nil {
		return dbus.UnixFD(-1), &dbus.Error{
			"com.docker.Error.NoPty",
			[]interface{}{"No pty in this container"}}
	}

	return dbus.UnixFD(init.ptyMaster.Fd()), nil
}

func (init *DockerInit) GetStdOut() (dbus.UnixFD, dbus.UnixFD, *dbus.Error) {
	init.Lock()
	defer init.Unlock()

	return dbus.UnixFD(init.stdout.Fd()), dbus.UnixFD(init.stderr.Fd()), nil
}

func (init *DockerInit) GetStdIn() (dbus.UnixFD, *dbus.Error) {
	init.Lock()
	defer init.Unlock()

	if init.stdin == nil {
		return dbus.UnixFD(-1), &dbus.Error{
			"com.docker.Error.NoStdin",
			[]interface{}{"Stdin is closed"}}
	}

	res := dbus.UnixFD(init.stdin.Fd())

	// Only give stdin to the first caller and then close it on our
	// side.  This gives the docker daemon the power to close the
	// app's stdin in StdinOnce mode.
	if init.openStdin {
		init.stdin.Close()
		init.stdin = nil
	}

	return res, nil
}

func (init *DockerInit) GotConnection(server dbus.Server, conn *dbus.Conn) {
	init.Lock()
	defer init.Unlock()
	conn.Export(init, DockerInitPath, DockerInitInterface)
	conn.Export(init.introspectable, DockerInitPath, "org.freedesktop.DBus.Introspectable")
	// TODO: handle close and remove these
	if err := conn.ServerAuth(nil, server.Uuid()); err != nil {
		conn.Close()
		return
	}
	init.connections = append(init.connections, conn)

	// Always emit the "last" state change if you connect to a container so that we can
	// keep track of state changes atomically
	conn.Emit(DockerInitPath, "com.docker.DockerInit.StateChanged", init.state, init.error, init.exitStatus)
}

// Caller must hold lock
func (init *DockerInit) changeState(state State, err string, exitStatus int32) {
	init.state = state
	init.error = err
	init.exitStatus = exitStatus
	for _, conn := range init.connections {
		conn.Emit(DockerInitPath, "com.docker.DockerInit.StateChanged", init.state, err, exitStatus)
	}
}

func dockerInitNew(args *DockerInitArgs) *DockerInit {
	init := &DockerInit{
		exitStatus: -1,
		openStdin:  args.openStdin,
		resume:     make(chan int),
	}

	introspectData := &introspect.Node{
		Name: DockerInitPath,
		Interfaces: []introspect.Interface{
			introspect.IntrospectData,
			introspect.Interface{
				Name:    DockerInitInterface,
				Methods: introspect.Methods(init),
				Signals: []introspect.Signal{
					{
						Name: "StateChanged",
						Args: []introspect.Arg{
							{"state", "i", "out"},
							{"error", "s", "out"},
							{"exit_status", "i", "out"},
						},
					},
				},
			},
		},
	}

	init.introspectable = introspect.NewIntrospectable(introspectData)

	return init
}

func runDbusServer(init *DockerInit) error {
	path := dbusSocketPath()
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}

	server, err := dbus.NewServer("unix:path="+path /* uuid */, "1234567890123456")
	if err != nil {
		return err
	}

	go dbus.Serve(server, init)

	return nil
}

func setupHostname(args *DockerInitArgs) error {
	hostname := getEnv(args, "HOSTNAME")
	if hostname == "" {
		return nil
	}
	return syscall.Sethostname([]byte(hostname))
}

func setupNetworking(args *DockerInitArgs) error {
	if args.ip != "" {
		// eth0
		iface, err := net.InterfaceByName("eth0")
		if err != nil {
			return fmt.Errorf("Unable to set up networking: %v", err)
		}
		ip, ipNet, err := net.ParseCIDR(args.ip)
		if err != nil {
			return fmt.Errorf("Unable to set up networking: %v", err)
		}
		if err := netlink.NetworkLinkAddIp(iface, ip, ipNet); err != nil {
			return fmt.Errorf("Unable to set up networking: %v", err)
		}
		if err := netlink.NetworkLinkUp(iface); err != nil {
			return fmt.Errorf("Unable to set up networking: %v", err)
		}

		// loopback
		if iface, err = net.InterfaceByName("lo"); err != nil {
			return fmt.Errorf("Unable to set up networking: %v", err)
		}
		if err := netlink.NetworkLinkUp(iface); err != nil {
			return fmt.Errorf("Unable to set up networking: %v", err)
		}
	}
	if args.gateway != "" {
		gw := net.ParseIP(args.gateway)
		if gw == nil {
			return fmt.Errorf("Unable to set up networking, %s is not a valid gateway IP", args.gateway)
		}

		if err := netlink.AddDefaultGw(gw); err != nil {
			return fmt.Errorf("Unable to set up networking: %v", err)
		}
	}

	return nil
}

func getCredential(args *DockerInitArgs) (*syscall.Credential, error) {
	if args.user == "" {
		return nil, nil
	}
	userent, err := utils.UserLookup(args.user)
	if err != nil {
		return nil, fmt.Errorf("Unable to find user %v: %v", args.user, err)
	}

	uid, err := strconv.Atoi(userent.Uid)
	if err != nil {
		return nil, fmt.Errorf("Invalid uid: %v", userent.Uid)
	}
	gid, err := strconv.Atoi(userent.Gid)
	if err != nil {
		return nil, fmt.Errorf("Invalid gid: %v", userent.Gid)
	}

	return &syscall.Credential{Uid: uint32(uid), Gid: uint32(gid)}, nil
}

func setupCapabilities(args *DockerInitArgs) error {

	if args.privileged {
		return nil
	}

	drop := []capability.Cap{
		capability.CAP_SETPCAP,
		capability.CAP_SYS_MODULE,
		capability.CAP_SYS_RAWIO,
		capability.CAP_SYS_PACCT,
		capability.CAP_SYS_ADMIN,
		capability.CAP_SYS_NICE,
		capability.CAP_SYS_RESOURCE,
		capability.CAP_SYS_TIME,
		capability.CAP_SYS_TTY_CONFIG,
		capability.CAP_MKNOD,
		capability.CAP_AUDIT_WRITE,
		capability.CAP_AUDIT_CONTROL,
		capability.CAP_MAC_OVERRIDE,
		capability.CAP_MAC_ADMIN,
	}

	c, err := capability.NewPid(os.Getpid())
	if err != nil {
		return err
	}

	c.Unset(capability.CAPS|capability.BOUNDS, drop...)

	if err := c.Apply(capability.CAPS | capability.BOUNDS); err != nil {
		return err
	}
	return nil
}

func setupCommon(args *DockerInitArgs) error {

	if err := setupHostname(args); err != nil {
		return err
	}

	if err := setupNetworking(args); err != nil {
		return err
	}

	if err := setupCapabilities(args); err != nil {
		return err
	}

	return nil
}

func getEnv(args *DockerInitArgs, key string) string {
	for _, kv := range args.env {
		parts := strings.SplitN(kv, "=", 2)
		if parts[0] == key && len(parts) == 2 {
			return parts[1]
		}
	}
	return ""
}

func getCmdPath(args *DockerInitArgs) (string, error) {

	// Set PATH in dockerinit so we can find the cmd
	if envPath := getEnv(args, "PATH"); envPath != "" {
		os.Setenv("PATH", envPath)
	}

	// Find the cmd
	cmdPath, err := exec.LookPath(args.args[0])
	if err != nil {
		if args.workDir == "" {
			return "", err
		}
		if cmdPath, err = exec.LookPath(path.Join(args.workDir, args.args[0])); err != nil {
			return "", err
		}
	}

	return cmdPath, nil
}

func babySit(process *os.Process) int {

	// Forward all signals to the app
	sigchan := make(chan os.Signal, 1)
	utils.CatchAll(sigchan)
	go func() {
		for sig := range sigchan {
			if sig == syscall.SIGCHLD {
				continue
			}
			process.Signal(sig)
		}
	}()

	// Wait for the app to exit.  Also, as pid 1 it's our job to reap all
	// orphaned zombies.
	var wstatus syscall.WaitStatus
	for {
		var rusage syscall.Rusage
		pid, err := syscall.Wait4(-1, &wstatus, 0, &rusage)
		if err == nil && pid == process.Pid {
			break
		}
	}

	return wstatus.ExitStatus()
}

// Run as pid 1 in the typical Docker usage: an app container that doesn't
// need its own init process.  Running as pid 1 allows us to monitor the
// container app and return its exit code.
func dockerInitApp(args *DockerInitArgs) error {
	init := dockerInitNew(args)
	init.Lock()
	defer init.Unlock()

	// Prepare the cmd based on the given args
	// If this fails we report that below
	cmdPath, cmdErr := getCmdPath(args)
	cmd := exec.Command(cmdPath, args.args[1:]...)
	cmd.Dir = args.workDir
	cmd.Env = args.env

	// App runs in its own session
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	// Console setup.  Hook up the container app's stdin/stdout/stderr to
	// either a pty or pipes.  The FDs for the controlling side of the
	// pty/pipes will be passed to docker later via a UNIX socket.
	if args.tty {
		ptyMaster, ptySlave, err := pty.Open()
		if err != nil {
			return err
		}
		init.ptyMaster = ptyMaster
		cmd.Stdout = ptySlave
		cmd.Stderr = ptySlave
		if args.openStdin {
			cmd.Stdin = ptySlave
			cmd.SysProcAttr.Setctty = true
		}
	} else {
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			return err
		}
		init.stdout = stdout.(*os.File)

		stderr, err := cmd.StderrPipe()
		if err != nil {
			return err
		}
		init.stderr = stderr.(*os.File)
		if args.openStdin {
			// Can't use cmd.StdinPipe() here, since in Go 1.2 it
			// returns an io.WriteCloser with the underlying object
			// being an *exec.closeOnce, neither of which provides
			// a way to convert to an FD.
			pipeRead, pipeWrite, err := os.Pipe()
			if err != nil {
				return err
			}
			cmd.Stdin = pipeRead
			init.stdin = pipeWrite
		}
	}

	if err := runDbusServer(init); err != nil {
		// Can't report error here, as dbus is not up...
		return err
	}

	// Wait for docker to tell us to start
	init.Unlock() // Allow calls
	<-init.resume
	init.Lock()

	exitCode := 1

	if cmdErr != nil {
		init.changeState(FailedToStart, cmdErr.Error(), -1)
	} else {
		// Container setup
		if err := setupCommon(args); err != nil {
			init.changeState(FailedToStart, err.Error(), -1)
		} else {
			// Start the app
			if err = cmd.Start(); err != nil {
				init.changeState(FailedToStart, err.Error(), -1)
			} else {
				init.process = cmd.Process
				init.changeState(Running, "", -1)

				init.Unlock() // Allow calls
				exitCode = babySit(init.process)
				init.Lock()
				init.changeState(Exited, "", int32(exitCode))
			}
		}
	}

	init.Unlock() // Allow calls

	// Wait for docker to call Resume() again.  This gives docker a chance
	// to get the exit code from the RPC socket call interface before we
	// die.
	select {
	case <-init.resume:
	case <-time.After(time.Second):
		return fmt.Errorf("timeout waiting for docker Resume()")
	}

	init.Lock()

	os.Exit(exitCode)
	return nil
}

// Runs as pid 1 when starting a machine container that has its own init
// process.  Start the dockerinit child, do some container setup, and then exec
// the real init.
func dockerInitMachineParent(args *DockerInitArgs) error {

	// Create a pty slave to be used by the container for its console.  The
	// pty master will be owned by the dockerinit child process.
	ptyMaster, ptySlave, err := pty.Open()
	if err != nil {
		return err
	}
	defer ptyMaster.Close()
	defer ptySlave.Close()

	// Hook up /dev/console to the pty with a bind mount
	if err := syscall.Mount(ptySlave.Name(), "/dev/console", "", syscall.MS_BIND, ""); err != nil {
		return err
	}

	// Container setup
	if err := setupCommon(args); err != nil {
		return err
	}

	// Hook up stdin/stdout/stderr to the pty
	fd := int(ptySlave.Fd())
	if err := syscall.Dup2(fd, 0); err != nil {
		return err
	}
	if err := syscall.Dup2(fd, 1); err != nil {
		return err
	}
	if err := syscall.Dup2(fd, 2); err != nil {
		return err
	}

	// Prepare to receive a signal from the child dockerinit
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGUSR1)

	// Prepare to start the long-running dockerinit child via the
	// dockerinit "-child" option
	cmdArgs := append([]string{"-child"}, os.Args[1:]...)
	cmd := exec.Command(os.Args[0], cmdArgs...)

	// Pass the pty master FD to the child dockerinit so that it can access
	// the parent's console
	cmd.ExtraFiles = []*os.File{ptyMaster}

	// Put child dockerinit in its own session so that it doesn't get a
	// signal when e.g. systemd does TIOCNOTTY
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	// Set the child uid/gid credentials if needed.  Not sure if this
	// really makes sense for a machine container, but if the user asked
	// for it...
	credential, err := getCredential(args)
	if err != nil {
		return err
	}
	cmd.SysProcAttr.Credential = credential

	// Start the child
	if err := cmd.Start(); err != nil {
		return err
	}

	// Wait for signal to continue from the child
	<-sigchan
	signal.Stop(sigchan)

	// Exec the container's real init process
	path, err := exec.LookPath(args.args[0])
	if err != nil {
		return err
	}
	return syscall.Exec(path, args.args, args.env)
}

// Long-running non-pid-1 dockerinit for the machine container case.  Started
// by dockerInitMachineParent().
func dockerInitMachineChild(args *DockerInitArgs) error {
	init := dockerInitNew(args)
	init.Lock()
	defer init.Unlock()

	var err error
	init.process, err = os.FindProcess(1)
	if err != nil {
		return err
	}

	// Create the dockerInitConsole struct and pass it the ptyMaster that
	// was sent by dockerInitMachineParent()
	init.ptyMaster = os.NewFile(3, "ptyMaster")

	if err := runDbusServer(init); err != nil {
		// Can't report error here, as dbus is not up...
		return err
	}

	// Wait for docker to tell us to start
	init.Unlock() // Allow calls
	<-init.resume
	init.Lock()

	// We're ready now.  Tell dockerInitMachineParent() to exec the real init.
	if err := init.process.Signal(syscall.SIGUSR1); err != nil {
		return err
	}

	init.changeState(Running, "", -1)

	init.Unlock() // Allow calls
	// Sleep forever while the servers run...
	for {
		time.Sleep(time.Hour)
	}
	init.Lock()
	return nil
}

// Sys Init code
// This code is run INSIDE the container and is responsible for setting
// up the environment before running the actual process
func SysInit() {
	if len(os.Args) <= 1 {
		fmt.Println("You should not invoke dockerinit manually")
		os.Exit(1)
	}

	// Get cmdline arguments
	user := flag.String("u", "", "username or uid")
	gateway := flag.String("g", "", "gateway address")
	workDir := flag.String("w", "", "workdir")
	ip := flag.String("i", "", "ip address")
	privileged := flag.Bool("privileged", false, "privileged mode")
	tty := flag.Bool("tty", false, "use pseudo-tty")
	openStdin := flag.Bool("stdin", false, "open stdin")
	child := flag.Bool("child", false, "is child dockerinit")
	flag.Parse()

	// Get env
	var env []string
	content, err := ioutil.ReadFile("/.dockerenv")
	if err != nil {
		log.Fatalf("Unable to load environment variables: %v", err)
	}
	if err := json.Unmarshal(content, &env); err != nil {
		log.Fatalf("Unable to unmarshal environment variables: %v", err)
	}

	// Propagate the plugin-specific container env variable
	env = append(env, "container="+os.Getenv("container"))

	args := &DockerInitArgs{
		user:       *user,
		gateway:    *gateway,
		workDir:    *workDir,
		ip:         *ip,
		privileged: *privileged,
		tty:        *tty,
		openStdin:  *openStdin,
		child:      *child,
		env:        env,
		args:       flag.Args(),
	}

	if args.child {
		// Machine container child
		err = dockerInitMachineChild(args)
	} else if path.Base(args.args[0]) == "systemd" || args.args[0] == "/sbin/init" {
		// Machine container parent
		err = dockerInitMachineParent(args)
	} else {
		// Typical docker usage: app container
		err = dockerInitApp(args)
	}

	if err != nil {
		log.Fatal(err)
	}
}
