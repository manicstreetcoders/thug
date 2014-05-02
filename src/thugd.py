#!/usr/bin/env python
"""
Thug daemon

By thorsten.sick@avira.com
For the iTES project (www.ites-project.org)
"""

import argparse
import pika
import sys
import time
import json
import threading
import os
import signal

try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser

import subprocess
import os
import shutil


class Thugd():
    """ 
        A class waiting for jobs, starting thug, returning results
    """
    def __init__(self, configfile, clear = False):
        """
        @configfile:    The configuration file to use
        @clear:         Clear the job chain
        """
        self.clear = clear
        self.username = "guest"
        self.password = "guest"
        self._read_config(configfile)
        self._run_queue()

    def _read_config(self, configfile):
        """
        read_config

        Read configuration from configuration file

        @configfile: The configfile to use
        """
        self.host   = "localhost"
        self.queue  = "thugctrl"
        self.rhost  = "localhost"
        self.rqueue = "thugres"

        if configfile is None:
            return

        conf = ConfigParser()
        conf.read(configfile)
        self.host   = conf.get("jobs", "host")
        self.queue  = conf.get("jobs", "queue")
        self.rhost  = conf.get("results", "host")
        self.rqueue = conf.get("results", "queue")
        self.resdir = conf.get("results", "resdir")
        self.username   = conf.get("credentials", "username")
        self.password  = conf.get("credentials", "password")

    def _run_queue(self):
        credentials = pika.PlainCredentials(self.username, self.password)
        parameters = pika.ConnectionParameters(host = self.host, credentials = credentials)
        connection = pika.BlockingConnection(parameters)
        channel    = connection.channel()

        channel.queue_declare(queue = self.queue, durable = True)
        print("[*] Waiting for messages on %s %s (press CTRL+C to exit)" % (self.host, self.queue, ))

        channel.basic_qos(prefetch_count = 1)
        channel.basic_consume(lambda c, m, p, b: self.callback(c, m, p, b), queue = self.queue)
        channel.start_consuming()

    def enqueue_output(self, out, queue):
        for line in iter(out.readline, b''):
            queue.put(line)
        out.close()

    def runProcess(self, exe, timeout=300):
        try:
            from Queue import Queue, Empty
        except ImportError:
            from queue import Queue, Empty

        ON_POSIX = 'posix' in sys.builtin_module_names

        start = time.time()
        last_io = start
        end = start + timeout
        interval = 0.10

        p = subprocess.Popen(exe, 
            bufsize=1,
            close_fds=ON_POSIX,
            stdout=subprocess.PIPE)
        q = Queue()
        t = threading.Thread(target=self.enqueue_output, args=(p.stdout,q))
        t.daemon = True
        t.start()

        while True:
            retcode = p.poll()
            if retcode is not None:
                print "[x] Child thug.py died."
                break;
            if time.time() >= end:
                try:
                    print "[x] Sending SIGKILL..."
                    os.kill(p.pid, signal.SIGKILL)
                except OSError:
                    pass
            try: line = q.get_nowait()
            except Empty:
                if time.time() > last_io + 10:
                    try:
                        print "[x] Sending SIGINT..."
                        os.kill(p.pid, signal.SIGINT)
                        last_io = time.time()
                    except OSError:
                        pass
            else:
                last_io = time.time()
                yield line
            time.sleep(interval)
        
        last_io = time.time()
        while True:
            try: line = q.get_nowait()
            except Empty:
                if time.time() > last_io + 10:
                    break
            else:
                last_io = time.time()
                yield line
            time.sleep(interval)

    def send_results(self, data):
        credentials = pika.PlainCredentials(self.username, self.password)
        parameters = pika.ConnectionParameters(host = self.rhost, credentials = credentials)
        connection = pika.BlockingConnection(parameters)
        channel    = connection.channel()

        channel.queue_declare(queue = self.rqueue, durable = True)

        message = json.dumps(data)
        channel.basic_publish(exchange    = '',
                              routing_key = self.rqueue,
                              body        = message,
                              properties  = pika.BasicProperties(delivery_mode = 2,))

        print ("[x] Sent %r" % (message,))
        connection.close()

    def copy_to_result(self, frompath, job):
        """
        Copy result folder to result path
        """

        if not frompath:
            return None

        respath = os.path.join(self.resdir, str(job["id"]))
        self.copytree(frompath, respath)
        return os.path.relpath(respath, self.resdir)

    def copytree(self, src, dst, symlinks=False, ignore=None):
        if not os.path.exists(dst):
            os.makedirs(dst)
        for item in os.listdir(src):
            s = os.path.join(src,item)
            d = os.path.join(dst,item)
            if os.path.isdir(s):
                self.copytree(s, d, symlinks, ignore)
            else:
                if not os.path.exists(d) or os.stat(src).st_mtime - os.stat(dst).st_mtime > 1:
                    shutil.copy2(s,d)

    def process(self, job):
        """
        Execute thug to process a job
        """
        print("job" + str(job))

        command = ["python", "thug.py", "--useragent=win7ie80"]

        if job["extensive"]:
            command.append("-E")
        if job["referer"]:
            command.append("-r")
            command.append(job["referer"])
        if job["proxy"]:
            command.append("-p")
            command.append(job["proxy"])

        command.append(job["url"])
        print(command)

        pathname = None

        if job["timeout"]:
            timeout = job["timeout"]
        else:
            timeout = 300

        for line in self.runProcess(command, timeout):
            if line.startswith("["):
                print "* %s" % line

            if line.find("] Saving log analysis at ") >= 0:
                pathname = line.split(" ")[-1].strip()

        rpath = self.copy_to_result(pathname, job)
        res = {"id"     : job["id"],
               "rpath"  : rpath}

        self.send_results(res)

    def callback(self, ch, method, properties, body):
        print("[x] Received %r" % (body, ))

        if not self.clear:
            self.process(json.loads(body))

        print("[x] Done")

        ch.basic_ack(delivery_tag=method.delivery_tag)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'Receives jobs and starts Thug to process them')
    parser.add_argument('--config', help = 'Configuration file to use', default = "config.ini")
    parser.add_argument('--clear', help = 'Clear the job chain', default = False, action = "store_true")
    args = parser.parse_args()

    try:
        t = Thugd(args.config, args.clear)
    except KeyboardInterrupt:
        sys.exit(0)
