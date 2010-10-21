# -*- coding: utf-8 -*-

import memcache

# XXX where best to specify timeout?  constructor or various methods?

class KestrelEnqueueException(Exception):
    pass

class connection(object):
    def __init__(self, servers, queue, reliable=True,
                 default_timeout=0, fanout_key=None):
        if fanout_key == None:
            self.__queue = queue
        else:
            self.__queue = "%s+%s" % (queue, fanout_key)
            pass
        
        self.__reliable = reliable

        if default_timeout == 0:
            self.__timeout_suffix = ""
        else:
            self.__timeout_suffix = "t=%d" % default_timeout
            pass
                
        if reliable:
            self.dequeue = self.__reliable_read_fn
            self.dequeue_finish = self.__reliable_finish_read_fn
            self.dequeue_abort = self.__reliable_abort_read_fn
            self.enqueue = self.__reliable_write_fn
            
            self.__reliable_read_key = "%s/open/%s" % \
                (self.__queue, self.__timeout_suffix)
            self.__reliable_close_key = "%s/close" % self.__queue
            self.__reliable_abort_key = "%s/abort" % self.__queue
        else:
            self.dequeue = None # self.__unreliable_read_fn
            self.dequeue_finish = None # self.__unreliable_finish_read_fn
            self.dequeue_abort = None # self.__unreliable_abort_read_fn
            self.enqueue = None
            pass
        
        self.__mc = memcache.Client(servers, allow_get_timeouts=True, debug=1)
        
        pass

    def __reliable_write_fn(self, value):
        ret = self.__mc.set(self.__queue, value)
        if ret == 0:
            raise KestrelEnqueueException()
        pass
    
    def __reliable_read_fn(self, timeout=0):
        # FIXME timeout belongs here, somehow
        key = "%s/t=%d" % (self.__queue, timeout)
        l = self.__mc.get(key)
        return l
    
    def __reliable_finish_read_fn(self):
        l = self.__mc.get(self.__reliable_close_key)
        pass
    
    def __reliable_abort_read_fn(self):
        l = self.__mc.get(self.__reliable_abort_key)
        pass
    
    # TODO: write the unreliable equivalents some day
    pass
