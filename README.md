# OCR and automated analysis pipeline

This project is an implementation of the open-source OCR project *Tesseract* using a the serverless Azure Functions, with a series of (growing) analyses of the text derived, as well as the original image. The aim is to produce transcriptions and descriptive metadata for each image.

To begin with, the final output will be a file that collates the metadata generated into  a draft finding aid that can be verified before uploading to Archive Space.

## Naming convention

The image pipeline uses Azure blob storage to store the images and any derivatives. The following naming convention for each blob object has been implemented:

`image-pipeline/<operation>/<box-reference>/<filename>`

## Operations

### OCR

Performs OCR on an image. The image must be supplied as a file_path in an Azure data lake blob.
