package com.capitalone.gallery.utils;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.Tag;
import gherkin.deps.com.google.gson.Gson;
import gherkin.deps.com.google.gson.reflect.TypeToken;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Cat3Cat1TransferUtils {
    private static final Logger logger = LoggerFactory.getLogger(Cat3Cat1TransferUtils.class);

    public enum TagBasedAction {
        VOLTRON_COPY, CAT2_COPY, NONE
    }

    public static final String VOLTRON_MOVE_SUCCESS = "Success: Bundle Encrypted and Moved to Voltron Bucket";
    public static final String VOLTRON_MOVE_FAILURE = "Failure: Bundle Failed During Encryption / Move to Voltron Bucket";
    public static final String CAT2_MOVE_SUCCESS = "Success: Bundle Moved to CAT2 Bucket";
    public static final String CAT2_MOVE_FAILURE = "Failure: Bundle Failed During Move to CAT2 Bucket";
    public static final String NO_ACTION_TAKEN = "No Action Taken.";

    public String doActionForTags(String s3bucket, String s3ObjectKey) {
        AmazonS3 amazonS3 = AwsClientUtils.getAmazonS3Client();
        List<Tag> tagsForBundle = TaggingUtils.getTagsForBundle(amazonS3, s3bucket, s3ObjectKey);

        logger.info("Bundle tags present: {}", tagsForBundle.stream()
                .map(t -> String.format("%s=%s", t.getKey(), t.getValue())).collect(Collectors.toList()));

        TagBasedAction actionToTake = getActionToTake(s3ObjectKey, tagsForBundle);

        if (actionToTake == TagBasedAction.VOLTRON_COPY) {
            return doVoltronCopy(s3bucket, s3ObjectKey, tagsForBundle);
        } else if (actionToTake == TagBasedAction.CAT2_COPY) {
            return doEncryptAndCat3ToCat2Copy(s3bucket, s3ObjectKey, tagsForBundle);
        } else {
            return NO_ACTION_TAKEN;
        }
    }

    private String readResourceAsString(String filePath) {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(filePath)) {
            return IOUtils.toString(is, StandardCharsets.UTF_8);
        } catch (IOException e) {
            logger.error("Exception occurred while trying to read config file {}: {}", filePath, e);
            throw new RuntimeException();
        }
    }

    private List<String> convertJsonArrayToStringList(String jsonFilePath){
        Gson converter = new Gson();
        String jsonString = readResourceAsString(jsonFilePath);
        Type type = new TypeToken<List<String>>(){}.getType();

        return converter.fromJson(jsonString, type);
    }

    private TagBasedAction getActionToTake(String s3ObjectKey, List<Tag> tagsForBundle) {
        String actionForLambda = System.getenv("TAG_BASED_ACTION");

        boolean isCat3Bundle = s3ObjectKey.contains("CAT3_BUNDLE/");
        boolean voltronProcessingTagPresent = TaggingUtils.tagExistsWithValue(tagsForBundle, "VOLTRON-PROCESSING",
                "SUCCESS");

        List<String> patternsToIgnoreForTransfer = convertJsonArrayToStringList("file_configs/file-pattern-ignore-transfer.json");
        boolean isIgnoredFile = isInFilePatterns(s3ObjectKey, patternsToIgnoreForTransfer);

        if (isCat3Bundle && !isIgnoredFile && actionForLambda.equals("VOLTRON_COPY")) {
            return TagBasedAction.VOLTRON_COPY;
        } else if (!voltronProcessingTagPresent && !isIgnoredFile && actionForLambda.equals("CAT2_COPY")) {
            return TagBasedAction.CAT2_COPY;
        } else {
            return TagBasedAction.NONE;
        }
    }

    private boolean isInFilePatterns(String s3ObjectKey, List<String> filePatterns) {
        String fileName = Paths.get(s3ObjectKey).getFileName().toString();
        for (String patternString : filePatterns) {
            Pattern pattern = Pattern.compile(patternString);
            Matcher matcher = pattern.matcher(fileName);

            if (matcher.matches()) {
                return true;
            }
        }

        return false;
    }

    private String doEncryptAndCat3ToCat2Copy(String s3bucket, String s3ObjectKey, List<Tag> tagsForBundle) {
        String outcome = CAT2_MOVE_SUCCESS;

        logger.info("Beginning encryption and cat2 file transfer action.");
        byte[] encryptedBundle = null;
        ByteArrayInputStream fileContentToWrite = null;
        try {
            encryptedBundle = getEncryptedBundleContents(s3bucket, s3ObjectKey);
            long contentLength = encryptedBundle.length;
            fileContentToWrite = new ByteArrayInputStream(encryptedBundle);
            moveBundleToCat2Bucket(s3bucket, s3ObjectKey, fileContentToWrite, contentLength, tagsForBundle);
        } catch (Exception e) {
        	logger.info("Exception occurred during CAT2 File transfer: {}", Paths.get(s3ObjectKey).getFileName().toString());
            logger.info("Exception occurred while performing the encryption/cat2 transfer: ", e);
            outcome = CAT2_MOVE_FAILURE;
        } finally {
            encryptedBundle = null;
            if (null != fileContentToWrite) {
                try {
                    fileContentToWrite.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    outcome = CAT2_MOVE_FAILURE;
                }
            }
        }

        return outcome;
    }

    private void moveBundleToCat2Bucket(String sourceBucket, String sourceKey, InputStream fileContentToWrite, long contentLength, List<Tag> currentTags) throws IOException {
        String s3TargetBucket = System.getenv("CAT_2_BUCKET");
        String targetKeyName = "ASVAWSIMAGING/CAT3_BUNDLE/" + Paths.get(sourceKey).getFileName().toString();

        logger.info("Downloading S3 bundle for transfer.");
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentLength(contentLength);

        PutObjectRequest putObjectRequest = new PutObjectRequest(s3TargetBucket, targetKeyName, fileContentToWrite, objectMetadata);
        putObjectRequest.setTagging(new ObjectTagging(currentTags));

        AmazonS3 cat2AmazonS3Client = AwsClientUtils.getCat2AmazonS3Client();
        cat2AmazonS3Client.putObject(putObjectRequest);

        logger.info("Bundle copied from s3:{} to s3:{}", Paths.get(sourceBucket, sourceKey).toString(),
                Paths.get(s3TargetBucket, targetKeyName).toString());
        
        logFileDetails(Paths.get(sourceKey).getFileName().toString(), contentLength);
    }

    private S3Object downloadS3Bundle(String s3SourceBucket, String s3ObjectKey) {
        return AwsClientUtils.getAmazonS3Client().getObject(s3SourceBucket, s3ObjectKey);
    }
    
    private void logFileDetails(String fileName, long contentLength) {
    	
        logger.info("File transferred from CAT3 to CAT2: {}",  fileName);
        
    	int sizeInMB = (int) contentLength / 1024 / 1024;
    	if(sizeInMB > 500) {
    		logger.info("File {} exceeding the limit of 500 MB. Transferred File Size : {} MB",  fileName, sizeInMB);
    	} else {
    		logger.info("File {} below the limit of 500 MB. Transferred File Size : {} MB",  fileName, sizeInMB);
    	}
    	
    }

    private String doVoltronCopy(String s3bucket, String s3ObjectKey, List<Tag> tagsForBundle) {
        logger.info("Beginning copy to voltron staging folder.");

        try {
            moveBundleToVoltronBucket(s3bucket, s3ObjectKey, tagsForBundle);
            logger.info("Voltron file transfer completed successfully");

            return VOLTRON_MOVE_SUCCESS;
        } catch (Exception e) {
            logger.error("An error occurred while sending bundle to Voltron", e);
            return VOLTRON_MOVE_FAILURE;
        }

    }


    private byte[] getEncryptedBundleContents(String s3bucket, String s3ObjectKey)
            throws Exception {
        List<String> patternsToIgnoreForEncryption = convertJsonArrayToStringList("file_configs/file-pattern-ignore-encryption.json");
        try (S3Object s3Object = downloadS3Bundle(s3bucket, s3ObjectKey)) {
            byte[] fileContent;
            if(isInFilePatterns(s3ObjectKey, patternsToIgnoreForEncryption)){
                logger.info("Bundle is already encrypted. Skipping encryption step...");
                try (InputStream s3ObjectContent = s3Object.getObjectContent()) {
                    fileContent = IOUtils.toByteArray(s3ObjectContent);
                }
            } else {
                logger.info("Getting encrypted contents...");
                try (ByteArrayOutputStream os = SimplePGPUtil.encryptUsingGPG(s3Object)) {
                    fileContent = os.toByteArray();
                }
            }
            logger.info("S3 Object copied into new BAOS with length: {}", fileContent.length);
            return fileContent;
        } catch (Exception e) {
            logger.info("Exception occurred while downloading /Encrypting docs from bucket: ", e);
            throw e;
        }
    }

    private void moveBundleToVoltronBucket(String s3SourceBucket, String s3SourceObjectKey, List<Tag> tagsForBundle) throws IOException {
        AmazonS3 amazonS3 = AwsClientUtils.getAmazonS3Client();

        String s3TargetBucket = System.getenv("VOLTRON_BUCKET");
        String targetKeyName = Paths.get(System.getenv("VOLTRON_PREFIX") + Paths.get(s3SourceObjectKey).getFileName().toString()).toString();

        if (!TaggingUtils.tagExistsWithValue(tagsForBundle, "CAT3-BUNDLE", "TRUE")) {
            tagsForBundle.add(new Tag("CAT3-BUNDLE", "TRUE"));
        }

        logger.info("Downloading S3 bundle for transfer.");
        try (S3Object s3Object = downloadS3Bundle(s3SourceBucket, s3SourceObjectKey);
             InputStream fileContent = s3Object.getObjectContent()) {
            PutObjectRequest putObjectRequest = new PutObjectRequest(s3TargetBucket, targetKeyName, fileContent, new ObjectMetadata());
            putObjectRequest.setTagging(new ObjectTagging(tagsForBundle));

            amazonS3.putObject(putObjectRequest);
        }

        logger.info("Bundle copied from s3:{} to s3:{}", Paths.get(s3SourceBucket, s3SourceObjectKey).toString(),
                Paths.get(s3TargetBucket, targetKeyName).toString());
        amazonS3.deleteObject(new DeleteObjectRequest(s3SourceBucket, s3SourceObjectKey));
        logger.info("Source file deleted.");
    }
}
