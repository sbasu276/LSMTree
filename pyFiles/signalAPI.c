#define _GNU_SOURCE
#include <stdio.h>
#include <signal.h>

#define MAXNUMSIGNALS 64

//Array of function pointers
void *signal_handler_map[MAXNUMSIGNALS] (int fd);

void set_signal_handler(int signum, void* handler){

	signal_handler_map[signum] = handler;

	//assuming fcntl settings is done...
	struct sigaction act;

	act.sa_sigaction = my_handler;
	sigemptyset(&act.sa_mask);
	act.sa_flags = SA_SIGINFO;
	sigaction(signum, &act, NULL);
}

static void my_handler(int sig, siginfo_t *si, void *data) {
	//Definition of this function is in python part
	print("Received signal from FD %d", si->fd);
	(*signal_handler_map[sig]) (si->fd);
}
