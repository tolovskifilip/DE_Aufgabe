import pickle
import os.path
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import base64


def login():
    
    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']


    if os.path.exists('token.pickle'): #if the credentials have already been verified
            with open('token.pickle', 'rb') as token: #load
                creds = pickle.load(token)
    else: #if the credentials have not been verified
        flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
        creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token: #save token, so it can be loaded for another calling of the script
                pickle.dump(creds, token) #save
    
    service = build('gmail', 'v1', credentials=creds) #initialize the service
    
    return service



def find_messages(service_login):
    
    search_query = "subject:Your Report is ready" #query for searching by subject

    #searching by subject in INBOX in the already verified user
    messages_with_subject = service_login.users().messages().list(userId='me',labelIds = ['INBOX'], q=search_query).execute() 
    
    print('There have been found:',messages_with_subject.get('resultSizeEstimate'),'messages with the subject: Your Report is ready')
    
    #get the messages and not the number of messages found, which is printed separately
    messages_with_subject = messages_with_subject.get('messages') 
    
    return messages_with_subject




def download_attachments(messages,service_login):
    downloaded_files=[]
    for message in messages: #iterate through all the messages
        message_id=message['id'] #get the message id
        message_with_attachment = service_login.users().messages().get(userId='me', id=message_id).execute() #get message by messageID
        
        body = message_with_attachment['payload']['parts'] #extract the particular information
        
        attachment_id = body[1]['body']['attachmentId'] #body is two sized array, where the attachmentID is found under 'body'
        
        
        
        #get the responding attachment by attachment ID
        attachment = service_login.users().messages().attachments().get(userId='me', messageId=message_id,id=attachment_id).execute()
        table = attachment['data'] 
        file_data = base64.urlsafe_b64decode(table.encode('UTF-8')) #decode the data in byte-like object
        path = body[1]['filename'] #get the file name
        
        
        
        # check if it is a relevant file(with the assumption only Historical Reports are relevant for this task)
        if 'Historical-Report-GUSFacebook' in path: 
            with open(path, 'wb') as f:
                f.write(file_data) #save data as a separate file in its original format
                print('Downloaded is file: ',path)
                downloaded_files.append(path)
                
    return downloaded_files #return list of downloaded files with attachments