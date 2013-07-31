import sys, os
import stat
import httplib
import urlparse
import json
import logging

WEBHDFS_CONTEXT_ROOT='/webhdfs/v1'
TRUNK_SIZE = 1024*1024
DATANODES = {'data-1': '10.1.1.1', 
             'data-2': '10.1.1.2',  }

logger = logging.getLogger('webhdfs')

class HdfsException(Exception):
    pass

class WebHDFS(object):       
    ''' Class for accessing HDFS via WebHDFS 
    
        To enable WebHDFS in your Hadoop Installation add the following configuration
        to your hdfs_site.xml (requires Hadoop >0.20.205.0):
        
        <property>
             <name>dfs.webhdfs.enabled</name>
             <value>true</value>
        </property>  
        <property>
             <name>dfs.support.append</name>
             <value>true</value>
        </property>  
    
        see: https://issues.apache.org/jira/secure/attachment/12500090/WebHdfsAPI20111020.pdf
    '''
    
    def __init__(self, namenode_host, namenode_port, hdfs_username):
        self.namenode_host=namenode_host
        self.namenode_port = namenode_port
        self.username = hdfs_username
        self.http_client = None


    def mkdir(self, path):
        url = '%s?op=MKDIRS&user.name=%s' % (path, self.username)
        logger.debug('Create directory: %s' % path)
        self._reset_namenode_client()
        try:
            self._request(self.http_client, 'PUT', url)
        finally:
            self.http_client.close()


    def rmdir(self, path):
        url = '%s?op=DELETE&recursive=true&user.name=%s' % (path, self.username)
        logger.debug('Delete directory: %s' + path)
        self._reset_namenode_client()
        try:
            self._request(self.http_client, 'DELETE', url)
        finally:
            self.http_client.close()
     
     
    def copyFromLocal(self, source_path, target_path, replication=1, buffersize=4096, append=False):
        url = '%s?op=CREATE&overwrite=true&user.name=%s' % (target_path, self.username)
        if isinstance(source_path, basestring):
            source_file = open(source_path, 'r')
        else:
            source_file = source_path

        logger.debug('Copy %s to HDFS with path name %s' % (source_path, target_path))
        self._reset_namenode_client()
        try:
            response = self._request(self.http_client, 'PUT', url, 307)
            msg = response.msg
        finally:
            self.http_client.close()
        redirect_host, redirect_port, redirect_url = self._get_node_info(msg)
        redirect_url = redirect_url + "&replication="+str(replication) + "&buffersize="+str(buffersize)
        logger.debug('Send redirect to: host: %s, port: %s, path: %s ' % (redirect_host, redirect_port, redirect_url))
        redirect_client = self._get_http_client(redirect_host, redirect_port)
        if not append: 
			self._request(redirect_client, 'PUT', redirect_url, 201, source_file.read())
        else:
	        try:
	            if hasattr(source_file, 'multiple_chunks') and source_file.multiple_chunks():
	                # large file from Django form uploaded
	                self._request(redirect_client, 'PUT', redirect_url, 201, '')
	                for chunk in source_file.chunks():
	                    self.appendToFile(target_path, chunk)
	            elif isinstance(source_file, file) and hasattr(source_file, 'seek') and callable(source_file.seek):
	                # local file
	                source_file.seek(TRUNK_SIZE)
	                if source_file.read(1) != '':
	                    source_file.seek(0)
	                    self._request(redirect_client, 'PUT', redirect_url, 201, '')
	                    while True:
	                        chunk = source_file.read(TRUNK_SIZE)
	                        if chunk == '':
	                            break
	                        self.appendToFile(target_path, chunk)
	                else:
	                    self._request(redirect_client, 'PUT', redirect_url, 201, source_file.read())
	            else:
	                self._request(redirect_client, 'PUT', redirect_url, 201, source_file.read())
	        finally:
	            if hasattr(source_file, 'close') and callable(source_file.close):
	                source_file.close()
        redirect_client.close()

    def appendToFile(self, path, data, buffersize=4096):
        url = '%s?op=APPEND&user.name=%s' % (path, self.username)
        logger.debug('Append data to HDFS file %s' % path)
        self._reset_namenode_client()
        try:
            response = self._request(self.http_client, 'POST', url, 307)
            msg = response.msg
        finally:
            self.http_client.close()
        redirect_host, redirect_port, redirect_url = self._get_node_info(msg)
        redirect_url = redirect_url + '&buffersize='+str(buffersize) 
        logger.debug('Send redirect to: host: %s, port: %s, path: %s ' % (redirect_host, redirect_port, redirect_url))
        redirect_client = self._get_http_client(redirect_host, redirect_port)
        try:
            self._request(redirect_client, 'POST', redirect_url, 200, data)
        finally:
            redirect_client.close()


    def copyToLocal(self, source_path, target_path, buffersize=4096):
        url = '%s?op=OPEN&overwrite=true&user.name=%s' % (source_path, self.username)
        logger.debug('Copy %s to local %s' % (source_path, target_path))
        self._reset_namenode_client()
        try:
            response = self._request(self.http_client, 'GET', url, 307)
            msg = response.msg
            response_len = response.length
        finally:
            self.http_client.close()
        # if file is empty GET returns a response with length == NONE and
        # no msg['location']
        if response_len != None:
            redirect_host, redirect_port, redirect_url = self._get_node_info(msg)
            redirect_url = redirect_url + '&buffersize='+str(buffersize) 
            logger.debug('Send redirect to: host: %s, port: %s, path: %s ' % (redirect_host, redirect_port, redirect_url))
            redirect_client = self._get_http_client(redirect_host, redirect_port)
            try:
                response = self._request(redirect_client, 'GET', redirect_url)
                with open(target_path, 'w') as target_file:
                    while True:
                        chunk = reponse.read(TRUNK_SIZE)
                        if chunk == '':
                            break
                        target_file.write(chunk)
            finally:
                redirect_client.close()
        else:
            logger.warn('%s is empty' % source_path)
            with open(target_path, 'w'):
                pass


    def listdir(self, path):
        url = '%s?op=LISTSTATUS&user.name=%s' % (path, self.username)
        logger.debug('List directory: %s' % path)
        self._reset_namenode_client()
        try:
            response = self._request(self.http_client, 'GET', url)
            data_dict = json.loads(response.read())
        finally:
            self.http_client.close()
        files=[]        
        for i in data_dict['FileStatuses']['FileStatus']:
            files.append(i['pathSuffix'])        
        return files


    def open(self, path, offset=0, length=None):
        url = '%s?op=OPEN&user.name=%s&offset=%d' % (path, self.username, offset)
        if length is not None:
            url += '&length=%d' % length
        logger.debug('Open file %s' % path)
        self._reset_namenode_client()
        try:
            response = self._request(self.http_client, 'GET', url, 307)
            msg = response.msg
        finally:
            self.http_client.close()
        redirect_host, redirect_port, redirect_url = self._get_node_info(msg)
        logger.debug('Send redirect to: host: %s, port: %s, path: %s ' % (redirect_host, redirect_port, redirect_url))
        redirect_client = self._get_http_client(redirect_host, redirect_port)
        return self._request(redirect_client, 'GET', redirect_url)
    
    def getinfo(self, path):
        url = '%s?op=GETFILESTATUS&user.name=%s' % (path, self.username)
        logging.debug('Get file or directory info %s' % path)
        self._reset_namenode_client()
        try:
            response = self._request(self.http_client, 'GET', url, 200)
            return json.loads(response.read())
        finally:
            self.http_client.close()
            
    def getsize(self, path):
        return self.getinfo(path)['FileStatus']['length']
    
    def isfile(self, path):
        return self.getinfo(path)['FileStatus']['type'] == 'FILE'
    
    def isdir(self, path):
        return self.getinfo(path)['FileStatus']['type'] == 'DIRECTORY'

    def delete(self, path):
        url = '%s?op=DELETE&user.name=%s&recursive=true' % (path, self.username)
        logger.debug('Delete file or directory %s (recursive: true)' % path)
        self._reset_namenode_client()
        try:
            self._request(self.http_client, 'DELETE', url)
        finally:
            self.http_client.close()

    def _get_http_client(self, host, port, timeout=60):
        httpClient = httplib.HTTPConnection(host, port, timeout)
        return httpClient

    def _get_node_info(self, response_msg, replication=None):
        location = response_msg['location']
        result = urlparse.urlparse(location)
        host, port = result.netloc.split(':', 1)
        host = DATANODES.get(host, host)
        # Bug in WebHDFS 0.20.205 => requires param otherwise a NullPointerException is thrown
        url = result.path + '?' + result.query
        if replication:
            url +=  '&replication=%s' % str(replication) 

        return (host, port, url)

    def _reset_namenode_client(self, timeout=60):
        self.http_client = self._get_http_client(self.namenode_host, self.namenode_port, timeout)

    def _request(self, client=None, method='GET', url='', expected_code=200, body=None):
        if not url.startswith(WEBHDFS_CONTEXT_ROOT):
            url = WEBHDFS_CONTEXT_ROOT + url
        if client is None:
            client = self.http_client
        client.request(method, url, body, headers={})
        response = client.getresponse()
        if response.status != expected_code:
            msg = 'Failed to handle url %s with method %s, status: %s, msg: %s' % (url, method, response.status, response.msg)
            logger.error(msg)
            raise HdfsException(msg)
        return response

DFS = WebHDFS('10.1.1.3', 50070, 'feiyuw')

class HdfsFileWrapper(object):
    def __init__(self, path_name, blksize=1024*1024*2):
        self.is_string = False
        self.offset = 0
        self.blksize = blksize
        self.path_name = path_name
        self.length = DFS.getsize(self.path_name)
        if self.length is not None and self.length <= self.blksize:
            http_response = DFS.open(self.path_name, offset=0)
            try:
                # dump small zip to memory
                self.fp = StringIO()
                self.fp.write(http_response.read())
                self.fp.seek(0)
                self.is_string = True
            finally:
                http_response.close()
        else:
            self.fp = None
            self.is_string = False
                
    def __getitem__(self, key):
        data = self.read(self.blksize)
        if data:
            return data
        raise IndexError

    def __iter__(self):
        return self

    def seek(self, offset, whence=0):
        self.offset = offset
        if self.is_string:
            self.fp.seek(offset, whence)
        else:
            if whence == 2:
                if offset < 0:
                    offset = -offset
                self.offset = self.length - offset
                
    def next(self):
        data = self.read(self.blksize)
        if data:
            return data
        raise StopIteration
    
    def exits(self, path):
        pass
                
    def read(self, size=None):
        if self.is_string:
            if size is None:
                return self.fp.read()
            return self.fp.read(size)
        http_response = DFS.open(self.path_name, offset=self.offset, length=size)
        try:
            if size is None:
                self.offset = self.length
            else:
                self.offset += size
                if self.offset > self.length:
                    self.offset = self.length
            return http_response.read()
        finally:
            http_response.close()
                
    def tell(self):
        return self.offset


    def __getattr__(self, attr):
        return getattr(self.fp, attr)


def main(): 
    dfs = WebHDFS('10.1.1.3', 50070, 'feiyuw')
    dfs.mkdir('/books/Erlang')
    dfs.copyFromLocal(r'/home/feiyuw/Programming Erlang.pdf', r'/books/Erlang/Programming_Erlang.pdf')
    files = dfs.listdir('/books/Erlang')
    print files
    f = dfs.open(r'/books/Erlang/Programming_Erlang.pdf')
    with open('/home/feiyuw/xx.pdf', 'w') as local_f:
        while True:
            chunk = f.read(1024*1024)
            if chunk == '':
                break
            local_f.write(chunk)


