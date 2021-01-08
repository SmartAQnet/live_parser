from ftplib import FTP
from ftplib import all_errors
from io import StringIO
from time import sleep

import saqncredentials

server = saqncredentials.grimm.server
user = saqncredentials.grimm.user
pw = saqncredentials.grimm.pw


def getData(path=None, retry=True):

    try:
        if(path is None):
            ftp = FTP(server)
            ftp.login(user, pw)
            res = ftp.nlst()

        elif(len(path.split("/")) == 1):
            [folder] = path.split("/")
            ftp = FTP(server)
            ftp.login(user, pw)
            ftp.cwd(folder)
            res = ftp.nlst()
            res.sort()

        elif(len(path.split("/")) == 2):
            [folder, thing] = path.split("/")
            ftp = FTP(server)
            ftp.login(user, pw)
            ftp.cwd(folder + "/" + thing)
            res = ftp.nlst()
            res.sort()

        elif(len(path.split("/")) == 3):
            [folder, thing, file] = path.split("/")

            def customWriter(line):
                r.write(line + "\n")

            ftp = FTP(server)
            ftp.login(user, pw)
            ftp.cwd(folder + "/" + thing)
            r = StringIO()
            ftp.retrlines('RETR ' + file, customWriter)
            res = r.getvalue()
            r.close()
        ftp.quit()

    except all_errors:
        if(retry):
            sleep(5)
            print("too many ftp connections, trying again")
            res = getData(path, False)
        else:
            print("failed to connect ftp again, aborting.")
            return []

    return res
