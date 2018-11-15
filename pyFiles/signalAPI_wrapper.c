#include <python.h>

static void set_signal_handler(PyObject* self, PyObject* args)
{

    PyObject *signum;
    PyObject *callback;

    /*  parse the input*/
    if (!PyArg_UnpackTuple(args, "ref", 1, 2, signum, callback))
        return NULL;

    set_signal_handler(*signum, callback);

    /*  construct the output*/
    return Py_BuildValue("f", answer);
}

/*  define functions in module */
static PyMethodDef SigMethods[] =
{
     {"set_signal_handler", set_signal_handler, METH_VARARGS, "Register a signal handler with the signal"},
     {NULL, NULL, 0, NULL}
};

#if PY_MAJOR_VERSION >= 3
/* module initialization */
/* Python version 3*/
static struct PyModuleDef cModPyDem =
{
    PyModuleDef_HEAD_INIT,
    "set_signal_handler", "Some documentation",
    -1,
    SigMethods
};

PyMODINIT_FUNC
PyInit_sig_module(void)
{
    return PyModule_Create(&cModPyDem);

}

#else

/* module initialization */
/* Python version 2 */
PyMODINIT_FUNC
initcos_module(void)
{
    (void) Py_InitModule("sig_module", SigMethods);
}

#endif
