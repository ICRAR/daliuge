#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2015
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved
#
#    This library is free software; you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public
#    License as published by the Free Software Foundation; either
#    version 2.1 of the License, or (at your option) any later version.
#
#    This library is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#    Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#    MA 02111-1307  USA
#
from abc import abstractmethod, ABCMeta
from cStringIO import StringIO
import logging
import os
import urlparse
import warnings


logger = logging.getLogger(__name__)

class OpenMode:
    OPEN_WRITE, OPEN_READ = xrange(2)

class DataIO(object):
    """
    A class used to read/write data stored in a particular kind of storage in an
    abstract way. This base class simply declares a number of methods that
    deriving classes must actually implement to handle different storage
    mechanisms (e.g., a local filesystem or an NGAS server).

    An instance of this class represents a particular piece of data. Thus at
    construction time users must specify a storage-specific unique identifier
    for the data that this object handles (e.g., a filename in the case of a
    DataIO class that works with local filesystem storage, or a host:port/fileId
    combination in the case of a class that works with an NGAS server).

    Once an instance has been created it can be opened via its `open` method
    indicating an open mode. If opened with `OpenMode.OPEN_READ`, only read
    operations will be allowed on the instance, and if opened with
    `OpenMode.OPEN_WRITE` only writing operations will be allowed.
    """

    __metaclass__ = ABCMeta

    def __init__(self):
        self._mode = None

    def open(self, mode, **kwargs):
        """
        Opens the underlying storage where the data represented by this instance
        is stored. Depending on the value of `mode` subsequent calls to
        `self.read` or `self.write` will succeed or fail.
        """
        self._mode = mode
        self._desc = self._open(**kwargs)

    def write(self, data, **kwargs):
        """
        Writes `data` into the storage
        """
        if self._mode is None:
            raise ValueError('Writing operation attempted on closed DataIO object')
        if self._mode == OpenMode.OPEN_READ:
            raise ValueError('Writing operation attempted on write-only DataIO object')
        return self._write(data, **kwargs)

    def read(self, count, **kwargs):
        """
        Reads `count` bytes from the underlying storage.
        """
        if self._mode is None:
            raise ValueError('Reading operation attempted on closed DataIO object')
        if self._mode == OpenMode.OPEN_WRITE:
            raise ValueError('Reading operation attempted on write-only DataIO object')
        return self._read(count, **kwargs)

    def close(self, **kwargs):
        """
        Closes the underlying storage where the data represented by this
        instance is stored, freeing underlying resources.
        """
        if self._mode is None:
            return
        self._close()
        self._mode = None

    def isOpened(self):
        return self._mode is not None

    @abstractmethod
    def exists(self):
        """
        Returns `True` if the data represented by this DataIO exists indeed in
        the underlying storage mechanism
        """

    @abstractmethod
    def delete(self):
        """
        Deletes the data represented by this DataIO
        """

    @abstractmethod
    def _open(self, **kwargs): pass

    @abstractmethod
    def _read(self, count, **kwargs): pass

    @abstractmethod
    def _write(self, data, **kwargs): pass

    @abstractmethod
    def _close(self, **kwargs): pass

class NullIO(DataIO):
    """
    A DataIO that stores no data
    """

    def _open(self, **kwargs):
        return None

    def _read(self, count=4096, **kwargs):
        return None

    def _write(self, data, **kwargs):
        return len(data)

    def _close(self, **kwargs):
        pass

    def exists(self):
        return True

    def delete(self):
        pass

class ErrorIO(DataIO):
    """
    An DataIO method that throws exceptions if any of its methods is invoked
    """

    def _open(self, **kwargs):
        raise NotImplementedError()

    def _read(self, count=4096, **kwargs):
        raise NotImplementedError()

    def _write(self, data, **kwargs):
        raise NotImplementedError()

    def _close(self, **kwargs):
        raise NotImplementedError()

    def exists(self):
        raise NotImplementedError()

    def delete(self):
        raise NotImplementedError()

class MemoryIO(DataIO):
    """
    A DataIO class that reads/write from/into the StringIO object given at
    construction time
    """

    def __init__(self, buf, **kwargs):
        self._buf = buf

    def _open(self, **kwargs):
        if self._mode == OpenMode.OPEN_WRITE:
            return self._buf
        else:
            val = ''
            if self._buf:
                val = self._buf.getvalue()
            return StringIO(val)

    def _write(self, data, **kwargs):
        self._desc.write(data)
        return len(data)

    def _read(self, count=4096, **kwargs):
        return self._desc.read(count)

    def _close(self, **kwargs):
        if self._mode == OpenMode.OPEN_READ:
            self._desc.close()
        # If we're writing we don't close the descriptor because it's our
        # self._buf, which won't be readable afterwards

    def exists(self):
        return not self._buf.closed

    def delete(self):
        self._buf.close()

class FileIO(DataIO):

    def __init__(self, filename, **kwargs):
        super(FileIO, self).__init__()
        self._fnm = filename

    def _open(self, **kwargs):
        flag = 'r' if self._mode is OpenMode.OPEN_READ else 'w'
        return open(self._fnm, flag)

    def _read(self, count=4096, **kwargs):
        return self._desc.read(count)

    def _write(self, data, **kwargs):
        self._desc.write(data)
        return len(data)

    def _close(self, **kwargs):
        self._desc.close()

    def getFileName(self):
        return self._fnm

    def exists(self):
        return os.path.isfile(self._fnm)

    def delete(self):
        os.unlink(self._fnm)

class NgasIO(DataIO):
    '''
    A DataObject whose data is finally stored into NGAS. Since NGAS doesn't
    support appending data to existing files, we store all the data temporarily
    in a file on the local filesystem and then move it to the NGAS destination
    '''

    def __init__(self, hostname, fileId, port = 7777, ngasConnectTimeout=2, ngasTimeout=2):

        # Check that we actually have the NGAMS client libraries
        try:
            from ngamsPClient import ngamsPClient  # @UnusedImport @UnresolvedImport
        except:
            warnings.warn("No NGAMS client libs found, cannot use NgasIO")
            raise

        super(NgasIO, self).__init__()
        self._ngasSrv            = hostname
        self._ngasPort           = port
        self._ngasConnectTimeout = ngasConnectTimeout
        self._ngasTimeout        = ngasTimeout
        self._fileId             = fileId

    def _getClient(self):
        from ngamsPClient import ngamsPClient  # @UnresolvedImport
        return ngamsPClient.ngamsPClient(self._ngasSrv, self._ngasPort, self._ngasTimeout)

    def _open(self, **kwargs):
        if self._mode == OpenMode.OPEN_WRITE:
            # The NGAS client API doesn't have a way to continually feed an ARCHIVE
            # request with data. Thus the only way we can currently archive data
            # into NGAS is by accumulating it all on our side and finally
            # sending it over.
            self._buf = ''
            self._writtenDataSize = 0
        return self._getClient()

    def _close(self, **kwargs):
        client = self._desc
        if self._mode == OpenMode.OPEN_WRITE:
            reply, msg, _, _ = client._httpPost(
                     client.getHost(), client.getPort(), 'QARCHIVE',
                     'application/octet-stream', dataRef=self._buf,
                     pars=[['filename',self._fileId]], dataSource='BUFFER',
                     dataSize=self._writtenDataSize)
            self._buf = None
            if reply != 200:
                # Probably msg is not enough, we need to unpack the status XML doc
                # from the returning data and extract the real error message from
                # there
                raise Exception(msg)

        # Release the reference to _desc so the client object gets destroyed
        del self._desc

    def _read(self, count, **kwargs):
        # Read data from NGAS and give it back to our reader
        self._desc.retrieve2File(self._fileId, cmd="QRETRIEVE")

    def _write(self, data, **kwargs):
        self._buf += data
        self._writtenDataSize += len(data)
        return len(data)

    def exists(self):
        import ngamsLib  # @UnresolvedImport
        status = self._getClient().sendCmd('STATUS', pars=[['fileId', self._fileId]])
        return status.getStatus() == ngamsLib.ngamsCore.NGAMS_SUCCESS

    def delete(self):
        pass # We never delete stuff from NGAS

def IOForURL(url):
    """
    Returns a DataIO instance that handles the given URL for reading.
    """
    url = urlparse.urlparse(url)
    io = None
    if url.scheme == 'file':
        hostname = url.netloc
        filename = url.path
        if hostname == 'localhost' or hostname == '127.0.0.1' or \
           hostname == os.uname()[1]:
            io = FileIO(filename)
    elif url.scheme == 'null':
        io = NullIO()
    elif url.scheme == 'ngas':
        networkLocation = url.netloc
        if ':' in networkLocation:
            hostname, port = networkLocation.split(':')
        else:
            hostname = networkLocation
            port = 7777
        fileId = url.path
        io = NgasIO(hostname, fileId, port)

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug('I/O chosen for dataURL %s: %r' % (url, io))

    return io