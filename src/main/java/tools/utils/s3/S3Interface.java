package tools.utils.s3;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.util.logging.Level;
import java.util.logging.Logger;

public class S3Interface {

    private static final Logger LOGGER = Logger.getLogger(S3Interface.class.getName());

    private final S3Client client;

    public S3Interface(final String accessKeyId, final String secretAccessKey, final String sessionToken,
                       final Region region) {

        final AwsSessionCredentials sessionCreds = AwsSessionCredentials.create(accessKeyId, secretAccessKey,
                sessionToken);

        client = S3Client.builder().region(region).credentialsProvider(StaticCredentialsProvider.
                create(sessionCreds)).build();
    }

    public S3Interface(final String accessKeyId, final String secretAccessKey, final Region region) {

        final AwsBasicCredentials creds = AwsBasicCredentials.create(accessKeyId, secretAccessKey);

        client = S3Client.builder().region(region).credentialsProvider(StaticCredentialsProvider.
                create(creds)).build();
    }

    public void uploadObject(final String bucketName, final String objName, final byte[] content) {

        LOGGER.log(Level.INFO, "Uploading object " + objName + " to bucket " + bucketName);

        client.putObject(PutObjectRequest.builder().bucket(bucketName).key(objName).build(),
                RequestBody.fromBytes(content));

        LOGGER.log(Level.INFO, "Uploaded object " + objName + " to bucket " + bucketName);

    }


    public byte[] downloadObject(final String bucketName, final String objName) {

        LOGGER.log(Level.INFO, "Downloading object " + objName + " from bucket " + bucketName);

        final ResponseBytes response = client.getObject(GetObjectRequest.builder().bucket(bucketName).key(objName).
                build(), ResponseTransformer.toBytes());

        LOGGER.log(Level.INFO, "Downloaded object " + objName + " from bucket " + bucketName);

        return response.asByteArray();
    }
}
