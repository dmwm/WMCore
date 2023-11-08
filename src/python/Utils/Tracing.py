#!/usr/bin/env python

"""
Stuff to help with debugging from https://pymotw.com/3/sys/tracing.html

Use like this:

        import sys
        from Utils.Tracing import trace_calls_and_returns, dont_trace

        sys.settrace(trace_calls_and_returns)
        # Do the things you want to trace
        sys.settrace(dont_trace)

"""


def trace_calls(frame, event, arg, to_be_traced=None):
    if event != 'call':
        return
    co = frame.f_code
    func_name = co.co_name
    if func_name == 'write':
        return  # Ignore write() calls from printing
    line_no = frame.f_lineno
    filename = co.co_filename
    print('* Call to {} on line {} of {}'.format(
        func_name, line_no, filename))
    if func_name in to_be_traced:
        # Trace into this function
        return trace_lines
    return


def trace_calls_and_returns(frame, event, arg):
    co = frame.f_code
    func_name = co.co_name
    if func_name == 'write':
        return  # Ignore write() calls from printing
    line_no = frame.f_lineno
    filename = co.co_filename
    if event == 'call':
        print('Call to {} on line {} of {}'.format(func_name, line_no, filename))
        return trace_calls_and_returns
    elif event == 'return':
        print(' {} => {} ({})'.format(func_name, arg, filename))
    return


def trace_lines(frame, event, arg):
    if event != 'line':
        return
    co = frame.f_code
    func_name = co.co_name
    line_no = frame.f_lineno
    print('*  {} line {}'.format(func_name, line_no))


def dont_trace(frame, event, arg):
    return
