import com.google.api.gax.paging.Page;
import com.google.cloud.storage.*;
import com.google.common.collect.Lists;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class betterUnzipper {
  public static final String bucketName = "gda-store-japan-1";
  public static final String pathPrefixBeforePID = ".gda_data_GameAnalytics/store";
  public static final String pathPrefixBeforePIDDev = ".gda_data_GameAnalyticsDev/store";
  public static final Storage storage = StorageOptions.getDefaultInstance().getService();

  private static String getPathPrefxiBeforePID(String mod) {
    if (mod.equals("dev")) return pathPrefixBeforePIDDev;
    return pathPrefixBeforePID;
  }

  public static List<Blob> getAllCompressedFileNames(String pid, String category, String mod) {
    String prefix = String.join("/", getPathPrefxiBeforePID(mod), pid, category);

    Page<Blob> blobs = storage.list(
            bucketName, Storage.BlobListOption.prefix(prefix));

    List<Blob> fileNames = Lists.newArrayList(blobs.iterateAll())
            .stream().filter(f -> {
              String[] tmpArray = f.getName().split("\\.");
              return tmpArray[tmpArray.length - 1].equals("gz");
            })
            .collect(Collectors.toList());

    return fileNames;
  }

  public static String getRightCsvName(String blobName) {
    String[] fileName = blobName.split("/");
    String directFileName = fileName[fileName.length - 1].split("\\.")[0] + ".csv";
    String csvFileName = String.join("#", Arrays.copyOfRange(fileName, 0, fileName.length - 1)) + "#"+directFileName;

    return csvFileName;
  }

  public static String getDownloadFileName(String blobName) {
    String[] fileName = blobName.split("/");
    return "/home/dexian/decompress/" + String.join("#", fileName);
  }

  public static void process(List<Blob> files) {
    int i = 0;
    for (Blob b : files) {
      Path tmp = Paths.get(getDownloadFileName(b.getName()));

      String downloadedName = getDownloadFileName(b.getName());
      String uploadName = getRightCsvName(b.getName());

      System.out.println(downloadedName);
      System.out.println(uploadName);
      /*
      download
       */
      b.downloadTo(tmp);

      File downloaded = tmp.toFile();
      File decompressed = new File("/home/dexian/decompress/" + getRightCsvName(b.getName()));
      unzipper.decompress(downloaded, decompressed,"gz");


      /*
      upload
       */
      try {
        String uploadNameReal = String.join("/", getRightCsvName(b.getName()).split("#"));

        System.out.println("uploading...: " + uploadNameReal);
        BlobId blobId = BlobId.of(bucketName, uploadNameReal);
        BlobInfo blobInfo =
                BlobInfo.newBuilder(blobId).setContentType("text/plain").build();

        storage.create(blobInfo, new FileInputStream(decompressed));

      } catch (IOException e) {
        e.printStackTrace();
        System.exit(1);
      }
      System.out.println("deleting file...");
      downloaded.delete();
      decompressed.delete();

      System.out.println(
              "----------------------------------------------------------------------------------");
    }

    System.out.println("finished");
  }


  public static void main(String[] args) {
    List<Blob> files = getAllCompressedFileNames("19391", "uploads/virtualPurchases", "live");
    process(files);
  }
}
