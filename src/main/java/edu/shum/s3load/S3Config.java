package edu.shum.s3load;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.util.IOUtils;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.ToString;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.UUID;

@Configuration
@Setter
@ToString
public class S3Config {


  @Value("${s3.accessKey}")
  private String accessKey;
  @Value("${s3.secretKey}")
  private String secretKey;
  @Value("${s3.serviceEndpoint}")
  private String serviceEndpoint;
  @Value("${s3.region}")
  private String region;
  @Value("${s3.bucketName}")
  private String bucketName;

  @Value("${s3.folderName:dictionary}")
  private String folderName;

  @Bean
  public AmazonS3 amazonS3() {

    return AmazonS3ClientBuilder
      .standard()
      .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, region))
      .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
      .build();

  }

  @SneakyThrows
  @PostConstruct
  private void run(){

    final var pipedOutputStream = new PipedOutputStream();
    final var pipedInputStream = new PipedInputStream(pipedOutputStream);

    var t1 = new Thread(() -> {
      try {
        IOUtils.copy(
                Files.newInputStream(Path.of("/Users/a19045391/file examples/big_data copy.csv")),
                pipedOutputStream
        );
      } catch (IOException e) {
        e.printStackTrace();
      }
    });

    AmazonS3 client = amazonS3();

    var objectMetadata = new ObjectMetadata();
    objectMetadata.setExpirationTime(Date.from(Instant.now().plus(1, ChronoUnit.HOURS)));

    final var s3Out = new Thread(() -> {
      client.putObject(bucketName, wrapToFolder(UUID.randomUUID().toString()), pipedInputStream, objectMetadata);
    });

    t1.start();
    s3Out.start();
    t1.join();
    s3Out.join();

  }

  private String wrapToFolder(String id) {
    return folderName + "/" + id;
  }
}
