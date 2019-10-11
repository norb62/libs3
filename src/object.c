/** **************************************************************************
 * object.c
 *
 * Copyright 2008 Bryan Ischo <bryan@ischo.com>
 *
 * This file is part of libs3.
 *
 * libs3 is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation, version 3 or above of the License.  You can also
 * redistribute and/or modify it under the terms of the GNU General Public
 * License, version 2 or above of the License.
 *
 * In addition, as a special exception, the copyright holders give
 * permission to link the code of this library and its programs with the
 * OpenSSL library, and distribute linked combinations including the two.
 *
 * libs3 is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * version 3 along with libs3, in a file named COPYING.  If not, see
 * <http://www.gnu.org/licenses/>.
 *
 * You should also have received a copy of the GNU General Public License
 * version 2 along with libs3, in a file named COPYING-GPLv2.  If not, see
 * <http://www.gnu.org/licenses/>.
 *
 ************************************************************************** **/

#include <stdlib.h>
#include <string.h>
#include "libs3.h"
#include "request.h"


// put object ----------------------------------------------------------------

void S3_put_object(const S3BucketContext *bucketContext, const char *key,
                   uint64_t contentLength,
                   const S3PutProperties *putProperties,
                   S3RequestContext *requestContext,
                   int timeoutMs,
                   const S3PutObjectHandler *handler, void *callbackData)
{
    // Set up the RequestParams
    RequestParams params =
    {
        HttpRequestTypePUT,                           // httpRequestType
        { bucketContext->hostName,                    // hostName
          bucketContext->bucketName,                  // bucketName
          bucketContext->protocol,                    // protocol
          bucketContext->uriStyle,                    // uriStyle
          bucketContext->accessKeyId,                 // accessKeyId
          bucketContext->secretAccessKey,             // secretAccessKey
          bucketContext->securityToken,               // securityToken
          bucketContext->authRegion },                // authRegion
        key,                                          // key
        0,                                            // queryParams
        0,                                            // subResource
        0,                                            // copySourceBucketName
        0,                                            // copySourceKey
        0,                                            // getConditions
        0,                                            // startByte
        0,                                            // byteCount
        putProperties,                                // putProperties
        handler->responseHandler.propertiesCallback,  // propertiesCallback
        handler->putObjectDataCallback,               // toS3Callback
        contentLength,                                // toS3CallbackTotalSize
        0,                                            // fromS3Callback
        handler->responseHandler.completeCallback,    // completeCallback
        callbackData,                                 // callbackData
        timeoutMs                                     // timeoutMs
    };

    // Perform the request
    request_perform(&params, requestContext);
}


// copy object ---------------------------------------------------------------


typedef struct CopyObjectData
{
    SimpleXml simpleXml;

    S3ResponsePropertiesCallback *responsePropertiesCallback;
    S3ResponseCompleteCallback *responseCompleteCallback;
    void *callbackData;

    int64_t *lastModifiedReturn;
    int eTagReturnSize;
    char *eTagReturn;
    int eTagReturnLen;

    string_buffer(lastModified, 256);
} CopyObjectData;


static S3Status copyObjectXmlCallback(const char *elementPath,
                                      const char *data, int dataLen,
                                      void *callbackData)
{
    CopyObjectData *coData = (CopyObjectData *) callbackData;

    int fit;

    if (data) {
        if (!strcmp(elementPath, "CopyObjectResult/LastModified")) {
            string_buffer_append(coData->lastModified, data, dataLen, fit);
        }
        else if (!strcmp(elementPath, "CopyObjectResult/ETag")) {
            if (coData->eTagReturnSize && coData->eTagReturn) {
                coData->eTagReturnLen +=
                    snprintf(&(coData->eTagReturn[coData->eTagReturnLen]),
                             coData->eTagReturnSize -
                             coData->eTagReturnLen - 1,
                             "%.*s", dataLen, data);
                if (coData->eTagReturnLen >= coData->eTagReturnSize) {
                    return S3StatusXmlParseFailure;
                }
            }
        }
    }

    /* Avoid compiler error about variable set but not used */
    (void) fit;

    return S3StatusOK;
}


static S3Status copyObjectPropertiesCallback
    (const S3ResponseProperties *responseProperties, void *callbackData)
{
    CopyObjectData *coData = (CopyObjectData *) callbackData;

    return (*(coData->responsePropertiesCallback))
        (responseProperties, coData->callbackData);
}


static S3Status copyObjectDataCallback(int bufferSize, const char *buffer,
                                       void *callbackData)
{
    CopyObjectData *coData = (CopyObjectData *) callbackData;

    return simplexml_add(&(coData->simpleXml), buffer, bufferSize);
}


static void copyObjectCompleteCallback(S3Status requestStatus,
                                       const S3ErrorDetails *s3ErrorDetails,
                                       void *callbackData)
{
    CopyObjectData *coData = (CopyObjectData *) callbackData;

    if (coData->lastModifiedReturn) {
        time_t lastModified = -1;
        if (coData->lastModifiedLen) {
            lastModified = parseIso8601Time(coData->lastModified);
        }

        *(coData->lastModifiedReturn) = lastModified;
    }

    (*(coData->responseCompleteCallback))
        (requestStatus, s3ErrorDetails, coData->callbackData);

    simplexml_deinitialize(&(coData->simpleXml));

    free(coData);
}


void S3_copy_object(const S3BucketContext *bucketContext, const char *key,
                    const char *destinationBucket, const char *destinationKey,
                    const S3PutProperties *putProperties,
                    int64_t *lastModifiedReturn, int eTagReturnSize,
                    char *eTagReturn, S3RequestContext *requestContext,
                    int timeoutMs,
                    const S3ResponseHandler *handler, void *callbackData)
{
    /* Use the range copier with 0 length */
    S3_copy_object_range(bucketContext, key,
                         destinationBucket, destinationKey,
                         0, NULL, // No multipart
                         0, 0, // No length => std. copy of < 5GB
                         putProperties,
                         lastModifiedReturn, eTagReturnSize,
                         eTagReturn, requestContext,
                         timeoutMs,
                         handler, callbackData);
}


void S3_copy_object_range(const S3BucketContext *bucketContext, const char *key,
                          const char *destinationBucket,
                          const char *destinationKey, const int partNo,
                          const char *uploadId, const unsigned long startOffset,
                          const unsigned long count,
                          const S3PutProperties *putProperties,
                          int64_t *lastModifiedReturn, int eTagReturnSize,
                          char *eTagReturn, S3RequestContext *requestContext,
                          int timeoutMs,
                          const S3ResponseHandler *handler, void *callbackData)
{
    // Create the callback data
    CopyObjectData *data =
        (CopyObjectData *) malloc(sizeof(CopyObjectData));
    if (!data) {
        (*(handler->completeCallback))(S3StatusOutOfMemory, 0, callbackData);
        return;
    }

    simplexml_initialize(&(data->simpleXml), &copyObjectXmlCallback, data);

    data->responsePropertiesCallback = handler->propertiesCallback;
    data->responseCompleteCallback = handler->completeCallback;
    data->callbackData = callbackData;

    data->lastModifiedReturn = lastModifiedReturn;
    data->eTagReturnSize = eTagReturnSize;
    data->eTagReturn = eTagReturn;
    if (data->eTagReturnSize && data->eTagReturn) {
        data->eTagReturn[0] = 0;
    }
    data->eTagReturnLen = 0;
    string_buffer_initialize(data->lastModified);

    // If there's a sequence ID > 0 then add a subResource, OTW pass in NULL
    char queryParams[512];
    char *qp = NULL;
    if (partNo > 0) {
        snprintf(queryParams, 512, "partNumber=%d&uploadId=%s", partNo, uploadId);
        qp = queryParams;
    }

    // Set up the RequestParams
    RequestParams params =
    {
        HttpRequestTypeCOPY,                          // httpRequestType
        { bucketContext->hostName,                    // hostName
          destinationBucket ? destinationBucket :
          bucketContext->bucketName,                  // bucketName
          bucketContext->protocol,                    // protocol
          bucketContext->uriStyle,                    // uriStyle
          bucketContext->accessKeyId,                 // accessKeyId
          bucketContext->secretAccessKey,             // secretAccessKey
          bucketContext->securityToken,               // securityToken
          bucketContext->authRegion },                // authRegion
        destinationKey ? destinationKey : key,        // key
        qp,                                           // queryParams
        0,                                            // subResource
        bucketContext->bucketName,                    // copySourceBucketName
        key,                                          // copySourceKey
        0,                                            // getConditions
        startOffset,                                  // startByte
        count,                                        // byteCount
        putProperties,                                // putProperties
        &copyObjectPropertiesCallback,                // propertiesCallback
        0,                                            // toS3Callback
        0,                                            // toS3CallbackTotalSize
        &copyObjectDataCallback,                      // fromS3Callback
        &copyObjectCompleteCallback,                  // completeCallback
        data,                                         // callbackData
        timeoutMs                                     // timeoutMs
    };

    // Perform the request
    request_perform(&params, requestContext);
}


// get object ----------------------------------------------------------------

void S3_get_object(const S3BucketContext *bucketContext, const char *key,
                   const S3GetConditions *getConditions,
                   uint64_t startByte, uint64_t byteCount,
                   S3RequestContext *requestContext,
                   int timeoutMs,
                   const S3GetObjectHandler *handler, void *callbackData)
{
    // Set up the RequestParams
    RequestParams params =
    {
        HttpRequestTypeGET,                           // httpRequestType
        { bucketContext->hostName,                    // hostName
          bucketContext->bucketName,                  // bucketName
          bucketContext->protocol,                    // protocol
          bucketContext->uriStyle,                    // uriStyle
          bucketContext->accessKeyId,                 // accessKeyId
          bucketContext->secretAccessKey,             // secretAccessKey
          bucketContext->securityToken,               // securityToken
          bucketContext->authRegion },                // authRegion
        key,                                          // key
        0,                                            // queryParams
        0,                                            // subResource
        0,                                            // copySourceBucketName
        0,                                            // copySourceKey
        getConditions,                                // getConditions
        startByte,                                    // startByte
        byteCount,                                    // byteCount
        0,                                            // putProperties
        handler->responseHandler.propertiesCallback,  // propertiesCallback
        0,                                            // toS3Callback
        0,                                            // toS3CallbackTotalSize
        handler->getObjectDataCallback,               // fromS3Callback
        handler->responseHandler.completeCallback,    // completeCallback
        callbackData,                                 // callbackData
        timeoutMs                                     // timeoutMs
    };

    // Perform the request
    request_perform(&params, requestContext);
}


// head object ---------------------------------------------------------------

void S3_head_object(const S3BucketContext *bucketContext, const char *key,
                    S3RequestContext *requestContext,
                    int timeoutMs,
                    const S3ResponseHandler *handler, void *callbackData)
{
    // Set up the RequestParams
    RequestParams params =
    {
        HttpRequestTypeHEAD,                          // httpRequestType
        { bucketContext->hostName,                    // hostName
          bucketContext->bucketName,                  // bucketName
          bucketContext->protocol,                    // protocol
          bucketContext->uriStyle,                    // uriStyle
          bucketContext->accessKeyId,                 // accessKeyId
          bucketContext->secretAccessKey,             // secretAccessKey
          bucketContext->securityToken,               // securityToken
          bucketContext->authRegion },                // authRegion
        key,                                          // key
        0,                                            // queryParams
        0,                                            // subResource
        0,                                            // copySourceBucketName
        0,                                            // copySourceKey
        0,                                            // getConditions
        0,                                            // startByte
        0,                                            // byteCount
        0,                                            // putProperties
        handler->propertiesCallback,                  // propertiesCallback
        0,                                            // toS3Callback
        0,                                            // toS3CallbackTotalSize
        0,                                            // fromS3Callback
        handler->completeCallback,                    // completeCallback
        callbackData,                                 // callbackData
        timeoutMs                                     // timeoutMs
    };

    // Perform the request
    request_perform(&params, requestContext);
}


// delete object --------------------------------------------------------------

void S3_delete_object(const S3BucketContext *bucketContext, const char *key,
                      S3RequestContext *requestContext,
                      int timeoutMs,
                      const S3ResponseHandler *handler, void *callbackData)
{
    // Set up the RequestParams
    RequestParams params =
    {
        HttpRequestTypeDELETE,                        // httpRequestType
        { bucketContext->hostName,                    // hostName
          bucketContext->bucketName,                  // bucketName
          bucketContext->protocol,                    // protocol
          bucketContext->uriStyle,                    // uriStyle
          bucketContext->accessKeyId,                 // accessKeyId
          bucketContext->secretAccessKey,             // secretAccessKey
          bucketContext->securityToken,               // securityToken
          bucketContext->authRegion },                // authRegion
        key,                                          // key
        0,                                            // queryParams
        0,                                            // subResource
        0,                                            // copySourceBucketName
        0,                                            // copySourceKey
        0,                                            // getConditions
        0,                                            // startByte
        0,                                            // byteCount
        0,                                            // putProperties
        handler->propertiesCallback,                  // propertiesCallback
        0,                                            // toS3Callback
        0,                                            // toS3CallbackTotalSize
        0,                                            // fromS3Callback
        handler->completeCallback,                    // completeCallback
        callbackData,                                 // callbackData
        timeoutMs                                     // timeoutMs
    };

    // Perform the request
    request_perform(&params, requestContext);
}

// restore object --------------------------------------------------------------

static const char *RestoreTiersString[] = {
   "Expedited",
   "Standard",
   "Bulk"
};


static S3Status generateRestoreXmlDocument(S3RestoreTier tier,
                                       int days,
                                       int *xmlDocumentLenReturn,
                                       char *xmlDocument,
                                       int xmlDocumentBufferSize)
{
    *xmlDocumentLenReturn = 0;

#define append(fmt, ...)                                        \
    do {                                                        \
        *xmlDocumentLenReturn += snprintf                       \
            (&(xmlDocument[*xmlDocumentLenReturn]),             \
             xmlDocumentBufferSize - *xmlDocumentLenReturn - 1, \
             fmt, __VA_ARGS__);                                 \
        if (*xmlDocumentLenReturn >= xmlDocumentBufferSize) {   \
            return S3StatusXmlDocumentTooLarge;                 \
        } \
    } while (0)

    append("<RestoreRequest><Days>%d</Days><GlacierJobParameters>"
           "<Tier>%s</Tier></GlacierJobParameters></RestoreRequest>",
           days,
           RestoreTiersString[tier]);

#undef append

    return S3StatusOK;
}

typedef struct RestoreXmlData
{
    S3ResponsePropertiesCallback *responsePropertiesCallback;
    S3ResponseCompleteCallback *responseCompleteCallback;
    void *callbackData;

    int xmlDocumentLen;
    const char *xmlDocument;
    int xmlDocumentBytesWritten;

} RestoreXmlData;

static S3Status RestoreDataPropertiesCallback
    (const S3ResponseProperties *responseProperties, void *callbackData)
{
    RestoreXmlData *paData = (RestoreXmlData *) callbackData;

    return (*(paData->responsePropertiesCallback))
        (responseProperties, paData->callbackData);
}

static int RestoreDataCallback(int bufferSize, char *buffer, void *callbackData)
{
    RestoreXmlData *paData = (RestoreXmlData *) callbackData;

    int remaining = (paData->xmlDocumentLen -
                     paData->xmlDocumentBytesWritten);

    int toCopy = bufferSize > remaining ? remaining : bufferSize;

    if (!toCopy) {
        return 0;
    }

    memcpy(buffer, &(paData->xmlDocument
                     [paData->xmlDocumentBytesWritten]), toCopy);

    printf("\nRestoreDataCallback: for %d return %s\n", toCopy, &paData->xmlDocument[paData->xmlDocumentBytesWritten]);

    paData->xmlDocumentBytesWritten += toCopy;

    return toCopy;
}

static void RestoreCompleteCallback(S3Status requestStatus,
                                   const S3ErrorDetails *s3ErrorDetails,
                                   void *callbackData)
{
    RestoreXmlData *paData = (RestoreXmlData *) callbackData;

    (*(paData->responseCompleteCallback))
        (requestStatus, s3ErrorDetails, paData->callbackData);
}

// Use a rather arbitrary max size for the document of 64K
#define S3_RESTORE_XML_DOC_MAXSIZE (64 * 1024)

void S3_restore_object(const S3BucketContext *bucketContext, const char *key,
                      S3RequestContext *requestContext,
                      int days,
                      S3RestoreTier tier,
                      int timeoutMs,
                      const S3ResponseHandler *handler, void *callbackData)
{
    string_buffer(generatedRestoreXmlDocument, S3_RESTORE_XML_DOC_MAXSIZE);

    S3Status generateXmlStatus = generateRestoreXmlDocument(tier, days,
                                       &generatedRestoreXmlDocumentLen,
                                       generatedRestoreXmlDocument,
                                       S3_RESTORE_XML_DOC_MAXSIZE);

    if (generateXmlStatus != S3StatusOK && handler && handler->completeCallback) {
        handler->completeCallback(generateXmlStatus, NULL, callbackData);
        return;
    }

    RestoreXmlData restoreXMLData;
    restoreXMLData.responsePropertiesCallback = handler->propertiesCallback;
    restoreXMLData.responseCompleteCallback = handler->completeCallback;
    restoreXMLData.callbackData = callbackData;

    restoreXMLData.xmlDocumentLen = generatedRestoreXmlDocumentLen;
    restoreXMLData.xmlDocument = generatedRestoreXmlDocument;
    restoreXMLData.xmlDocumentBytesWritten = 0;

    // Set up the RequestParams
    RequestParams params =
    {
        HttpRequestTypePOST,                        // httpRequestType
        { bucketContext->hostName,                    // hostName
          bucketContext->bucketName,                  // bucketName
          bucketContext->protocol,                    // protocol
          bucketContext->uriStyle,                    // uriStyle
          bucketContext->accessKeyId,                 // accessKeyId
          bucketContext->secretAccessKey,             // secretAccessKey
          bucketContext->securityToken,               // securityToken
          bucketContext->authRegion },                // authRegion
        key,                                          // key
        0,                                            // queryParams
        "restore",                                    // subResource
        0,                                            // copySourceBucketName
        0,                                            // copySourceKey
        0,                                            // getConditions
        0,                                            // startByte
        0,                                            // byteCount
        0,                                            // putProperties
        &RestoreDataPropertiesCallback,               // propertiesCallback
        &RestoreDataCallback,                         // toS3Callback
        generatedRestoreXmlDocumentLen,               // toS3CallbackTotalSize
        0,                                            // fromS3Callback
        &RestoreCompleteCallback,                     // completeCallback
        &restoreXMLData,                              // callbackData
        timeoutMs                                     // timeoutMs
    };

    (void) (params);
    (void) (requestContext);
    // Perform the request
    request_perform(&params, requestContext);
}
