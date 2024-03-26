# Carebot coding assignment

The aim of this assignment is to gauge your understanding and skills in database manipulation, interaction with cloud-based storage systems, image processing, and relevant coding languages.

## Befor you start

Please send us back IP of your computer from which you would like to connect to database. We will add it to the whitelist.

## Scenario

We have a large set of medical imaging data stored in a database, some images are annotated with different clinical conditions, and the actual image files are stored in Azure Blob Storage. We have a specific need to filter out certain images based on the annotations.

You will be working only with MMG (mamography) data, so you can ignore everything related to CXR(chest x-ray).

## Task

### Database Querying and Filtering

Connect to the given database (credentials were providet by e-mail), and write an SQL query to select scans (mmgscans_mmgscan.png_image) based on their annotations. Specifically, we are interested in images that have more than two annotations.

Every MMG study consists of 4 scans.

We are only interested in scans that have a double label match per study.
Interesting boolean annotation labels are:

-   has_benign_microcalsifications,
-   has_malign_microcalcifications,
-   has_benign_mass,
-   has_malign_mass.

You can ignore others.

For this task, you need to provide the SQL query you would use to filter these DICOMS.

Example:

rejected Study A

-   scan 1 annotation U1
    -   has_malign_mass =true
-   scan 2 annotation U1
    -   has_malign_mass =true
-   scan 3 annotation U1
    -   has_malign_mass =true
-   scan 4 annotation U1
    -   has_malign_mass =true
-   scan 1 annotation U2
    -   has_malign_mass =false
-   scan 2 annotation U2
    -   has_malign_mass =false
-   scan 3 annotation U2
    -   has_malign_mass =false
-   scan 4 annotation U2
    -   has_malign_mass =false

Accepted Study B

-   scan 1 annotation U1
    -   has_malign_mass =true
-   scan 2 annotation U1
    -   has_malign_mass =true
-   scan 3 annotation U1
    -   has_malign_mass =true
-   scan 4 annotation U1
    -   has_malign_mass =true
-   scan 1 annotation U2
    -   has_malign_mass =true
-   scan 2 annotation U2
    -   has_malign_mass =true
-   scan 3 annotation U2
    -   has_malign_mass =true
-   scan 4 annotation U2
    -   has_malign_mass =true

non existing Study C

-   scan 1 annotation U1
    -   has_malign_mass =true
-   scan 2 annotation U1
    -   has_malign_mass =true
-   scan 3 annotation U1
    -   has_malign_mass =false
-   scan 4 annotation U1
    -   has_malign_mass =false

### Image Retrieval from Azure Storage

Using the Azure Blob Storage SDK in Python, write a function that takes the list from the previous step, retrieves the corresponding images from Azure Blob Storage, and stores them in a local directory.

You should provide the complete function, including any necessary error handling and status logging. Remember to handle any potential issues such as network errors or missing files.

### Image Conversion

Write a function that reads each image file from the local directory (created in the previous step).
Visualize the bounding boxes on the images with description about the type of annotation.
Save the image in a separate directory acording the annotations.

You can use any image processing library you are comfortable with, but please provide a justification for your choice.

### Software Testing

The goal of this section is to assess your capability in ensuring the reliability and correctness of your code through testing.

Write tests for the functions you have created in the previous tasks, focusing on database querying and filtering, image retrieval.

All your test should be stored in tests directory.

## Note

Please ensure that your solution follows best practices for security and data privacy, especially when dealing with credentials and patient data.

## Launching in root folder
Main script:
    ```
    python3 -m src.scan
    ```

Test:
    ```
    python3 -m tests.test_db/test_azure_storage
    ```