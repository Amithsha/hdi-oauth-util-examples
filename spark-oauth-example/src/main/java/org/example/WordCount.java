package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import com.azure.security.keyvault.secrets.models.KeyVaultSecret;
import com.microsoft.azure.hdinsight.oauthtoken.credential.HdiIdentityTokenCredential;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;

public class WordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        // Ensure the correct number of arguments
        if (args.length != 2) {
            System.err.println("Usage: WordCount <input file> <output directory>");
            System.exit(1);
        }

        String inputFile = args[0];
        String outputDirectory = args[1];

        // Ensure the output directory exists
        File outputDir = new File(outputDirectory);
        if (!outputDir.exists()) {
            if (!outputDir.mkdirs()) {
                System.err.println("Failed to create output directory: " + outputDirectory);
                System.exit(1);
            }
        }

        // Spark Configuration and Context
        SparkConf conf = new SparkConf().setAppName("WordCountWithKeyVault").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String kvurl = "https://<YOUR_VAULT>.vault.azure.net/";
        try {
            // Initialize the Key Vault client
            HdiIdentityTokenCredential tokenCredential = new HdiIdentityTokenCredential("https://vault.azure.net/");
            SecretClient secretClient = new SecretClientBuilder()
                    .vaultUrl(kvurl)
                    .credential(tokenCredential)
                    .buildClient();

            // Retrieve the secret
            KeyVaultSecret secret = secretClient.getSecret("<YOUR_SECRET>");
            String secretValue = secret.getValue();
            System.out.println("Secret Value: " + secretValue);

            // Reading input file
            JavaRDD<String> input = sc.textFile(inputFile);

            // Transforming input to (word, 1) pairs
            JavaPairRDD<String, Integer> counts = input
                    .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                    .mapToPair(word -> new Tuple2<>(word, 1))
                    .reduceByKey(Integer::sum);

            // Save output to the specified directory
            counts.saveAsTextFile(outputDirectory);

            System.out.println("Word count completed. Output written to: " + outputDirectory);
        } catch (Exception e) {
            System.err.println("Error accessing Key Vault or processing data: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Stop the Spark Context
            sc.stop();
        }
    }
}
