#!/usr/bin/python
# Note: this script is based on the following script provided by Gogole itself:
# https://code.google.com/p/gdata-python-client/source/browse/samples/docs/docs_example.py
# .. and it is still WORK IN PROGRESS

import sys
import re
import os
import os.path
import getopt
import getpass

#from os import listdir
#from os.path import isfile, join
#from os import walk
import time

from apiclient import errors
from apiclient.http import MediaFileUpload

import gdata.docs.service
import gdata.spreadsheet.service
import gdata.data
import gdata.docs.client
import gdata.docs.data
import gdata.gauth

import yaml


class DocsSample(object):
  """A DocsSample object demonstrates the Document List feed."""

  def __init__(self, email, password):
    """Constructor for the DocsSample object.
    Takes an email and password corresponding to a gmail account to
    demonstrate the functionality of the Document List feed.
    Args:
      email: [string] The e-mail address of the account to use for the sample.
      password: [string] The password corresponding to the account specified by
          the email parameter.
    Returns:
      A DocsSample object used to run the sample demonstrating the
      functionality of the Document List feed.
    """
    source = 'Document List Python Sample'
    self.gd_client = gdata.docs.service.DocsService()
    self.gd_client.ClientLogin(email, password, source=source)

    # Setup a spreadsheets service for downloading spreadsheets
    self.gs_client = gdata.spreadsheet.service.SpreadsheetsService()
    self.gs_client.ClientLogin(email, password, source=source)


  def _GetFileExtension(self, file_name):
    """Returns the uppercase file extension for a file.
    Args:
      file_name: [string] The basename of a filename.
    Returns:
      A string containing the file extension of the file.
    """
    match = re.search('.*\.([a-zA-Z]{3,}$)', file_name)
    if match:
      return match.group(1).upper()
    return False

  def _GetFileExtension(self, file_name):
    """Returns the uppercase file extension for a file.
    Args:
      file_name: [string] The basename of a filename.
    Returns:
      A string containing the file extension of the file.
    """
    match = re.search('.*\.([a-zA-Z]{3,}$)', file_name)
    if match:
      return match.group(1).upper()
    return False
  
  
  def _google_folder(self, service, name, subfolder=None):
    folder_name_query = gdata.docs.service.DocumentQuery(categories=['folder'], params={'showfolders': 'true'})
    folder_name_query['title-exact'] = 'true'
    folder_name_query['title'] = name
    folder_feed = service.Query(folder_name_query.ToUri())
    if folder_feed.entry:
        return folder_feed.entry[0] 
        #return service.CreateFolder(name, subfolder)  
    return service.CreateFolder(name, subfolder)

  def _backupLog(self, updateText):
    """ Function used to log every new comment related to a specific File Upload to Google Drive."""
    aFile = "backup_log.txt"
    os.rename( aFile, aFile+"~")
    destination= open( aFile, "w" )
    source= open( aFile+"~", "r" )
    for line in source:
      destination.write( line )
    destination.write( str(updateText))
    source.close()
    destination.close()
    os.remove(aFile+"~")

  def _UploadMenu(self, myFilePath, myFileTitel):
    """Gets all the necessary data to upload a file to Google Drive."""

    file_path = myFilePath

    file_name = myFileTitel
    ext = self._GetFileExtension(file_name)


    if not ext or ext not in gdata.docs.service.SUPPORTED_FILETYPES:
      myUpdateText = str(file_name) + ' - File type not supported. Check the file extension.\n'
      print myUpdateText
      #update Backup Log file
      self._backupLog(myUpdateText)
      return
    else:
      content_type = gdata.docs.service.SUPPORTED_FILETYPES[ext]

    title = myFileTitel

    try:
      ms = gdata.MediaSource(file_path=file_path, content_type=content_type)
    except IOError:
      myUpdateText = str(file_name) + ' - If this is just a folder or if this is a .txt file - you can safely ignore this warning. Otherwise: Problems reading file. Check permissions.\n'
      print myUpdateText
      self._backupLog(myUpdateText)
      return

    if ext in ['CSV', 'ODS', 'XLS', 'XLSX']:
      myUpdateText = str(file_name) + ' - Uploading spreadsheet...\n'
      print myUpdateText
      self._backupLog(myUpdateText)
    elif ext in ['PPT', 'PPS']:
      myUpdateText = str(file_name) + ' - Uploading presentation...\n'
      print myUpdateText
      self._backupLog( myUpdateText)
    else:
      myUpdateText = str(file_name) +' - Uploading word processor document...\n'
      print myUpdateText
      self._backupLog( myUpdateText)

    folderName = 'backups' #+ str(time.strftime("%d/%m/%Y"))
    docs_service = self.gd_client
    b_folder = self._google_folder(docs_service, folderName)
    dest_folder = self._google_folder(docs_service, file_path, b_folder)

    entry = self.gd_client.Upload(ms, title, folder_or_uri=dest_folder)

    if entry:
      myUpdateText = str(file_name) +' - Upload successful!'
      self._backupLog( myUpdateText)
      myUpdateText = str(file_name) +' - Document now accessible at:', entry.GetAlternateLink().href
      self._backupLog( myUpdateText)
    else:
      myUpdateText = str(file_name) + ' - Upload error.'
      self._backupLog( myUpdateText)
    

  def _FilesToUpload(self):
  	"""Goes through files inside directory structure ."""
  	mypath = os.getcwd()
  	f = []
  	d = []
  	for (dirpath, dirnames, filenames) in walk(mypath):
  		f.extend(filenames)
  		if len(f) > 0:
  			for i in f:
  				if str(i) != 'backup_log.txt':
  					if str(i) != 'backup_log.txt~':
  						filePath = join(dirpath, i)
  						self._UploadMenu(filePath, i)
                
  def Run(self):
    """Executes the upload function."""
    self._backupLog('\n' + 'Backup date: ' + str(time.strftime("%d/%m/%Y")) + '\n' )
    self._backupLog('Backup Starting Time: ' + str(time.strftime("%H:%M:%S")) + '\n' )
    self._FilesToUpload()
    self._backupLog('\n' + 'Backup Ending Time: ' + str(time.strftime("%H:%M:%S")) + '\n' )
    mypath = os.getcwd()

def main():
  """Main function of the script that executes all necessary functions."""
  	# Parse command line options
  try:
  	 opts, args = getopt.getopt(sys.argv[1:], '', ['user=', 'pw='])
  except getopt.error, msg:
  	print 'python docs_example.py --user [username] --pw [password] '
  	sys.exit(2)

  stream = file('environment.yml', 'r')
  dict_ = yaml.load(stream)
  #print dict_

  myVars = {}
  for i in dict_:
    myVars[i] = dict_[i]

  user = myVars['list1']['account']
  pw = myVars['list1']['passwd']
  key = ''


  # Process options
  for option, arg in opts:
  	if option == '--user':
  	 user = arg
  	elif option == '--pw':
  	 pw = arg


  try:
  	sample = DocsSample(user, pw)
  except gdata.service.BadAuthentication:
  	print 'Invalid user credentials given.'
  	return

  sample.Run()


if __name__ == '__main__':
  main()
