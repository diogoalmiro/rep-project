import io
import os
import urllib.request
import html_text
from pdfminer.converter import TextConverter, HTMLConverter
from pdfminer.pdfinterp import PDFPageInterpreter
from pdfminer.pdfinterp import PDFResourceManager
from pdfminer.pdfpage import PDFPage

def extract_text_from_pdf( pdf_path ):
  text = ""
  resource_manager = PDFResourceManager()
  fake_file_handle = io.BytesIO()
  converter = HTMLConverter(resource_manager, fake_file_handle)
  page_interpreter = PDFPageInterpreter(resource_manager, converter)
  with open(pdf_path, 'rb') as fh:
     for page in PDFPage.get_pages(fh, caching=True, check_extractable=True): page_interpreter.process_page(page)
     text = fake_file_handle.getvalue().decode()
  converter.close()
  fake_file_handle.close()
  text = html_text.extract_text(text)
  file = open(pdf_path.replace('.pdf','.txt'), 'w')
  file.write(text)
  file.close()
        
def download_bte( year , number ):      
  filename = "bte" + repr(number) + "_" + repr(year) + ".pdf"
  download_url = "http://bte.gep.msess.gov.pt/completos/" + repr(year) + "/" + filename
  response = urllib.request.urlopen(download_url)
  file = open("../BTE-data/" + filename, 'wb')
  file.write(response.read())
  file.close()
  return filename

# Downloading data (e.g., from 1977 to 2020)
for year in range(2019, 2020):
  for number in range(1, 49):
    try: download_bte( year , number )
    except: print("Error processing number " + repr(number) + " from year " + repr(year) + "...")

# Converting all PDF data to TXT
for year in range(1977, 2020):
  for number in range(1, 49):
    try:
      filename = "bte" + repr(number) + "_" + repr(year) + ".pdf"
      extract_text_from_pdf( "../BTE-data/" + filename )
    except: print("Error processing number " + repr(number) + " from year " + repr(year) + "...")

# Compressing the PDF files
for fich in os.listdir('../BTE-data/'):
  fich = "../BTE-data/" + fich
  fich2 = "../BTE-data/" + fich + ".tmp"
  if fich[-3:]=="pdf": os.system("ps2pdf -dUseFlateCompression=true -dPDFSETTINGS=/screen %s %s" % (fich,fich2))
  if os.path.isfile(fich2):
    os.remove(fich)
    os.rename(fich2, fich)
