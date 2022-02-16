#!/usr/bin/python3

from googleapiclient.http import MediaFileUpload, MediaIoBaseUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from airflow.models import Variable
import json
import os

class gdrive():
    def __init__(self):
        # Set vars
        self.CLIENT_ID = Variable.get('CLIENT_ID')
        self.CLIENT_SECRET = Variable.get('CLIENT_SECRET')
        self.PROJECT_ID = Variable.get('PROJECT_ID')
        self.TOKEN_URI = 'https://oauth2.googleapis.com/token'
        self.AUTH_URI = 'https://accounts.google.com/o/oauth2/auth'
        self.AUTH_PROVIDER_X509_CERT_URL = 'https://www.googleapis.com/oauth2/v1/certs'
        self.REDIRECT_URIS = ['urn:ietf:wg:oauth:2.0:oob', 'http://localhost']
        self.SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly', 'https://www.googleapis.com/auth/drive.file'] # Modifying these scopes will require a new token
        self.PARENT_DIR = Variable.get('PARENT_DIR')
        # Authenticate credentials
        self.creds = Credentials(
            token=Variable.get('TOKEN'),
            refresh_token=Variable.get('REFRESH_TOKEN'),
            token_uri=self.TOKEN_URI,
            client_id=self.CLIENT_ID,
            client_secret=self.CLIENT_SECRET,
            scopes=self.SCOPES
        )

    def _refresh_token(self):
        # If there are no (valid) credentials available, let the user log in.
        if not self.creds or not self.creds.valid:
            if self.creds.expired and self.creds.refresh_token:
                self.creds.refresh(Request())
                # Update raw creds
                raw_creds = self.creds.to_json()
                Variable.set('token', raw_creds['TOKEN'])
                Variable.set('refresh_token', raw_creds['REFRESH_TOKEN'])
            else:
                flow = InstalledAppFlow.from_client_config(
                    client_config={
                        'installed': {
                            'client_id': self.CLIENT_ID,
                            'project_id': self.PROJECT_ID,
                            'auth_uri': self.AUTH_URI,
                            'token_uri': self.TOKEN_URI,
                            'auth_provider_x509_cert_url': self.AUTH_PROVIDER_X509_CERT_URL,
                            'client_secret': self.CLIENT_SECRET,
                            'redirect_uris': self.REDIRECT_URIS
                        }
                    },
                    scopes=self.SCOPES
                )
                self.creds = flow.run_local_server(port=0)

    def mkdir(self, name):
        # Refresh tokens if necessary
        self._refresh_token()
        # Initialize service
        service = build('drive', 'v3', credentials=self.creds)
        # Create file metadata
        metadata = {
            'parents': [self.PARENT_DIR],
            'name': name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        # Execute the creation
        folder = service.files().create(body=metadata, fields='id').execute()
        # Return the new folder's ID
        return folder.get('id')

    def upload_file(self, path_to_file, mimetype='text/plain'):
        # Refresh tokens if necessary
        self._refresh_token()
        # Initialize service
        service = build('drive', 'v3', credentials=self.creds)
        # Define file metadata and file media
        filename = path_to_file.split('/')[-1]
        metadata = {
            'parents': [self.PARENT_DIR],
            'name': filename,
            'mimeType': mimetype
        }
        media = MediaFileUpload(path_to_file, mimetype=mimetype, resumable=True)
        # Upload
        file = service.files().create(body=metadata, media_body=media, fields='id').execute()
        return file.get('id')

    def upload_buffer(self, buffer, filename, mimetype='text/plain'):
        # Refresh tokens if necessary
        self._refresh_token()
        # Initialize service
        service = build('drive', 'v3', credentials=self.creds)
        # Define file metadata and file media
        metadata = {
            'parents': [self.PARENT_DIR],
            'name': filename,
            'mimeType': mimetype
        }
        media = MediaIoBaseUpload(buffer, mimetype=mimetype, resumable=True)
        # Upload
        file = service.files().create(body=metadata, media_body=media, fields='id').execute()
        return file.get('id')
