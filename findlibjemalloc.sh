#!/bin/sh

# taken from bin/cassandra

find_library()
{
    pattern=$1
    path=$(echo ${2} | tr ":" " ")
    find $path -regex "$pattern" -print 2>/dev/null | head -n 1
}

case "`uname -s`" in
    Linux)
        which ldconfig > /dev/null 2>&1
        if [ $? = 0 ] ; then
            # e.g. for CentOS
            dirs="/lib64 /lib /usr/lib64 /usr/lib `ldconfig -v 2>/dev/null | grep -v '^\s' | sed 's/^\([^:]*\):.*$/\1/'`"
        else
            # e.g. for Debian, OpenSUSE
            dirs="/lib64 /lib /usr/lib64 /usr/lib `cat /etc/ld.so.conf /etc/ld.so.conf.d/*.conf | grep '^/'`"
        fi
        dirs=`echo $dirs | tr " " ":"`
        CASSANDRA_LIBJEMALLOC=$(find_library '.*/libjemalloc\.so\(\.1\)*' $dirs)
    ;;
    Darwin)
        CASSANDRA_LIBJEMALLOC=$(find_library '.*/libjemalloc\.dylib' $DYLD_LIBRARY_PATH:${DYLD_FALLBACK_LIBRARY_PATH-$HOME/lib:/usr/local/lib:/lib:/usr/lib})
    ;;
esac

if [ ! -z $CASSANDRA_LIBJEMALLOC ] ; then
    echo $(CASSANDRA_LIBJEMALLOC)
    exit 0
else
    exit 1
fi
