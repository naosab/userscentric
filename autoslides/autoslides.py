import os

from apiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials


class autoslides:
    """Class in order to create folders and updates slides presentation using `GDrive` and `GSlides` APIS.
        Example: `autoslides = autoslides(main_folder='DA_Automation', COMPANY='Contentsquare'`)
    """

    def __init__(self, main_folder='DA_Automation', CORE_USERNAME=None, COMPANY=None):
        """Init function to connect to `Drive` & `Slide` APIs. Store the main folder ID which defined by user.

        Args:
            main_folder (str, optional): Name of the principal folder where the folder / presentation will be created.
                                            Defaults to 'DA_Automation'.
            CORE_USERNAME ([type], optional): Core Username to add Analyst name to Presentation. Defaults to None.
            COMPANY ([type], optional): Company name to add company name in Presentation
                                            title and slides. Defaults to None.
        """
        self.SCOPES = ('https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/presentations',)
        self.CREDS = ServiceAccountCredentials.from_json_keyfile_name(
            f'{os.path.dirname(os.path.realpath(__file__))}/slidesautomation-311609-396da2e87622.json', self.SCOPES)
        self.DRIVE = build('drive',  'v3',  credentials=self.CREDS)
        self.SLIDE = build('slides', 'v1', credentials=self.CREDS)
        self.MAIN_FOLDER_ID = self.DRIVE.files().list(
                                q=f"mimeType='application/vnd.google-apps.folder' and name='{main_folder}'",
                                spaces='drive',
                                fields='nextPageToken, files(id, name)',
                                pageToken=None).execute()['files'][0]['id']
        self.SLIDE_REQUESTS = []
        self.COMPANY = COMPANY
        if CORE_USERNAME is not None:
            self.USER = CORE_USERNAME.replace('_', ' ').title()
        else:
            self.USER = None

    def get_parent_folder(self, parent_folder):
        """Get the id of the folder using folder name

        Args:
            parent_folder (str): Folder name of which we want to get the id

        Returns:
            [str]: String ID of the folder found
        """
        parent_id = self.DRIVE.files().list(
                        q=f"mimeType='application/vnd.google-apps.folder' and name='{parent_folder}'",
                        spaces='drive',
                        fields='nextPageToken, files(id, name)',
                        pageToken=None).execute()['files'][0]['id']

        return parent_id

    def create_folder(self, parent_id, folder_name):
        """Create an empty folder in the parent folder selected

        Args:
            parent_id (str): ID of the parent folder where we want to create a subfolder
            folder_name (str): Folder name which will be created

        Returns:
            [str]: ID of the folder created
        """

        file_metadata = {
            'name': f'{folder_name}',
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_id]
        }
        main_folder = self.DRIVE.files().create(body=file_metadata, fields='id').execute()

        return main_folder.get('id')

    def upload_file(self, parent_id, file_name, file_path, mimetype):
        """Upload a file stored locally into the selected Drive folder

        Args:
            parent_id (str): ID of the parent folder where the file is going to be uploaded
            file_path (str): Path to the local file which will be uploaded
            mimetype (str): Type of file uploaded, to find the type corresponding to the mimetype, please go to
                https://developers.google.com/drive/api/v3/ref-export-formats

        Returns:
            [str]: ID of the file uploaded
        """
        file_metadata = {
                        'name': file_name,
                        'parents': [parent_id]
                        }

        file_data = MediaFileUpload(f'{file_path}', mimetype=mimetype)
        file = self.DRIVE.files().create(body=file_metadata,
                                         media_body=file_data,
                                         fields='id').execute()
        return file.get('id')

    def create_presentation(self, template_id, deck_name, company=None, user=None, main_folder=None):
        """Duplicate a template presentation into the folder selected with provided name.

        Args:
            template_id (str): ID of the Slide template presentation
            deck_name (str): Name of the presentation (e.g: Checkout Analysis)
            company (str, optional): Company name if aplicable. Defaults to None.
            user (str, optional): User name of the person who's doing the analysis. Defaults to None.
            main_folder (str, optional): ID of the folder where the presentation will be created, if not provided
                it will take the folder picked when initializing the autoslides class. Defaults to None.

        Returns:
            [str]: ID of the new presentation duplicated with name formated as {deck_name} - {company} - {user}
        """

        if company is None:
            company = self.COMPANY
        if user is None:
            user = self.USER
        if main_folder is None:
            main_folder = self.MAIN_FOLDER_ID

        body = {
                'name': f'{deck_name} - {company} - {user}',
                'parents': [main_folder]
                }
        self.PRESENTATION_ID = self.DRIVE.files().copy(fileId=template_id, body=body).execute().get('id')

        return self.PRESENTATION_ID

    def replace_image(self, image_name, image_key, folder_image=None):
        """Replace the shape which contains the image_key with image in the Drive identified by it's name and the
        folder id. Current version might not work always. Proceed by changing the file permission to be accessible by
        anyone with link, get the link of the image, get the image and replace the shape which contains the key text.
        Finally restrict the access again of this file.

        Args:
            image_name (str): Full image name stored in the drive
            image_key (str): Text contained in the shape to be replaced.
            folder_image (str, optional): ID of the folder where the image is stored. Default to MAIN_FOLDER_ID.
        """

        folder_image = self.MAIN_FOLDER_ID if folder_image is None else folder_image
        img_id = self.DRIVE.files().list(
            q=f"name='{image_name}' and parents in '{folder_image}'").execute().get('files')[0]
        self.DRIVE.permissions().create(body={'role': 'reader', 'type': 'anyone'}, fileId=img_id['id']).execute()

        img_url = f'https://drive.google.com/uc?export=download&id={img_id["id"]}'

        reqs = [
            {
                'replaceAllShapesWithImage':
                    {
                        'imageReplaceMethod': 'CENTER_CROP',
                        'containsText':
                        {
                            'text': image_key,
                        },
                        'imageUrl': img_url
                    }
            }
        ]

        try:
            self.SLIDE.presentations().batchUpdate(body={'requests': reqs},
                                                   presentationId=self.PRESENTATION_ID).execute()
        except Exception as E:
            print(f"""Error trying to update image, you will need to add it manually (image available in Drive)
                  with code: {E}""")
            pass

        self.DRIVE.permissions().delete(fileId=img_id['id'], permissionId='anyoneWithLink').execute()

    def replace_text(self, fields={}, input=None, output=None):
        """Create requests for replacing text in slide either based on a dctionnary or by manually inputing the key
            and the value to replace. Store the requests in the autoslides class element `SLIDE_REQUESTS`.

        Args:
            fields (dict, optional): Dictionnary with key as the text to replace and value.
                with it's corresponding text replacement. Recommend to have all text keys and replacements values in
                one dictionnary for efficiency. Defaults to {}.
            input (str, optional): Key string corresponding of the text to replace. Defaults to None.
            output (str, optional): Replacement value of the text needed to be replaced. Defaults to None.
        """

        if input is None and output is None:
            for input, output in fields.items():
                request_replace = {
                    'replaceAllText':
                        {'containsText': {
                                'text': input
                            },
                            'replaceText': output
                         }
                        }

                self.SLIDE_REQUESTS.append(request_replace)
        else:
            request_replace = {
                'replaceAllText':
                    {
                        'containsText': {
                            'text': input
                        },
                        'replaceText': output
                    }
                    }
            self.SLIDE_REQUESTS.append(request_replace)

    def remove_slides(self, slides_index=[]):
        """Remove slides which are not needed by index and append the requests into the class element `SLIDE_REQUESTS`.

        Args:
            slides_index (list, optional): List containing the slide index of each slide needed to be removed.
                Remember indexing start at 0 in Python. Defaults to [].
        """

        presentation = self.SLIDE.presentations().get(presentationId=self.PRESENTATION_ID).execute()
        slides = presentation.get('slides')

        for slide_nb in slides_index:
            objectID = slides[int(slide_nb)].get('objectId')
            remove_req = {'deleteObject':
                          {
                            'objectId': objectID
                          }
                          }
            self.SLIDE_REQUESTS.append(remove_req)

    def update_presentation(self):
        """Run the `SLIDE_REQUESTS` stored in the class to update the presentation. It will clear the `SLIDE_REQUESTS`
            element of the autoslides class if the update is successful.
        """

        try:
            self.SLIDE.presentations().batchUpdate(
                body={'requests': self.SLIDE_REQUESTS},
                presentationId=self.PRESENTATION_ID).execute()
            self.SLIDE_REQUESTS = []
            print('Slides successfuly updated.')
        except Exception as E:
            print(f'Error while trying to update slides with the following details: {E}')

    def clear_request(self):
        """Clear the SLIDE_REQUESTS element of the autoslides class
        """
        self.SLIDE_REQUESTS = []

    def manual_add_requests(self, request):
        """Give the possibility to add to the SLIDE_REQUESTS elements a requested created manually for slides APIs
            stored in a dictionnary.

        Args:
            request (dict): Dictionnary containing the request wished to be added. To see full list of available
                requests in Google Slides APIs, please check:
                https://developers.google.com/slides/reference/rest/v1/presentations/request
        """

        self.SLIDE_REQUESTS.append(request)
