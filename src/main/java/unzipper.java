import com.google.api.gax.paging.Page;
import com.google.cloud.storage.*;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import javax.print.DocFlavor;
import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;


public class unzipper {

  private static Storage storage = StorageOptions.getDefaultInstance().getService();

  private static Iterable<Blob> listBlob(String bucketName, String prefix) {
    return storage
        .list(
            bucketName,
            Storage.BlobListOption.currentDirectory(),
            Storage.BlobListOption.prefix(prefix))
        .iterateAll();
  }

  private static Map<String, List<Blob>> getFiles(String bucketName, String prefix) {
    Map<String, List<Blob>> map = new LinkedHashMap<>();
    Map<String, List<Blob>> groupByPID = new LinkedHashMap<>();

    for (Blob b : listBlob(bucketName, prefix)) {
      for (Blob bb : listBlob(bucketName, b.getName())) {
        if (bb.getName().contains("/uploads/")) {
          for (Blob bbb : listBlob(bucketName, bb.getName())) {
            if (bbb.getName().contains("/virtualPurchases/")) {

              // finally reaches virtualPurchases level

              for (Blob compressedFile : listBlob(bucketName, bbb.getName())) {
                String[] nameArray = compressedFile.getName().split("\\.");
                String fileName = nameArray[1];

                map.putIfAbsent(fileName, new ArrayList<>());
                map.get(fileName).add(compressedFile);
              }
            }
          }
        }
      }
    }

    for (String k : map.keySet()) {

      String pid = k.split("/")[2];

      groupByPID.putIfAbsent(pid, new ArrayList<>());
      Collections.sort(map.get(k), Comparator.comparing(Blob::getSize, Comparator.reverseOrder()));
//      if (map.get(k).size() == 2) {
//        for (Blob blob : map.get(k)) {
//          System.out.println(blob);
//        }
//        System.out.println("==========================================");
//      }
      groupByPID.get(pid).add(map.get(k).get(0));
    }

    for (String k : groupByPID.keySet()) {
      System.out.printf("%s: %d\n", k, groupByPID.get(k).size());
      Collections.sort(groupByPID.get(k), Comparator.comparing(Blob::getName));
      System.out.println("------------------------------------------");
    }

    return groupByPID;
  }

  public static File decompress(File source, File dest, String format) {
    CompressorInputStream compressorStream;

    try {
      InputStream input = new FileInputStream(source);
      OutputStream output = new FileOutputStream(dest);
      switch (format) {
        case "bz2":
          {
            compressorStream = new BZip2CompressorInputStream(input);
            break;
          }

        case "gz":
          {
            compressorStream = new GzipCompressorInputStream(input);
            break;
          }

        default:
          throw new Exception("invalid compression scheme");
      }

      byte[] buffer = new byte[102400];
      int len;
      while ((len = compressorStream.read(buffer)) > 0) {
        output.write(buffer, 0, len);
      }

      compressorStream.close();
      output.close();

    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }

    return dest;
  }

  public static void main(String[] args) {
    String bucketName = "gda-store-japan-1";
    String prefix = ".gda_data_GameAnalytics/store/";
    Map<String, List<Blob>> map = getFiles(bucketName, prefix);

    List<String> alreadyDone = new ArrayList<>(Arrays.asList("19367"));

    map.forEach(
        (k, v) -> {
          if (!alreadyDone.contains(k)) {
            System.out.printf("starting %s\n", k);
            String[] bucketNameSplit = map.get(k).get(0).getName().split("/");

            String uploadFolderName =
                Arrays.stream(Arrays.copyOfRange(bucketNameSplit, 0, bucketNameSplit.length - 2))
                    .reduce("", (a, c) -> a + c + "/");

            uploadFolderName += "virtualPurchasesDecompressed/";
            System.out.println(uploadFolderName);

            File downloadFileName = new File("/home/dexian/explode_tmp/" + k);
            downloadFileName.mkdir();

            for (Blob blob : v) {
              String[] fileNameSplit = blob.getName().split("/");
              String[] formatSplit = fileNameSplit[fileNameSplit.length - 1].split("\\.");
              String format = formatSplit[formatSplit.length - 1];
              String uploadFileName =
                  fileNameSplit[fileNameSplit.length - 1].split("\\.")[0] + ".csv";
              String uploadFullPath = uploadFolderName + uploadFileName;

              String[] names = blob.getName().split("/");
              String p =
                  downloadFileName.getAbsolutePath().concat("/").concat(names[names.length - 1]);
              uploadFileName =
                  downloadFileName.getAbsolutePath().concat("/").concat(uploadFileName);
              Path temp = Paths.get(p);
              blob.downloadTo(temp);

              System.out.println("file downloaded as: " + p);
              System.out.println("file to upload: " + uploadFileName);
              System.out.println("file format: " + format);
              System.out.println("full path for gs upload: " + uploadFullPath);
              try {
                if (format.equals("csv")) throw new Exception(blob.toString());
              } catch (Exception e) {
                System.out.println(e.getMessage());
                System.exit(1);
              }

              File compressedFile = temp.toFile();
              File decompressedFile = new File(uploadFileName);
              decompress(compressedFile, decompressedFile, format);

              System.out.println("decompression succeed");

              // uploading to GCS
              try {
                System.out.println("uploading...");
                BlobId blobId = BlobId.of(bucketName, uploadFullPath);
                BlobInfo blobInfo =
                    BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
                new FileInputStream(decompressedFile);

                storage.create(blobInfo, new FileInputStream(decompressedFile));

              } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
              }
              System.out.println("deleting file...");
              compressedFile.delete();
              decompressedFile.delete();

              System.out.println(
                  "----------------------------------------------------------------------------------");
            }

            try {
              System.out.println("finished one, resting...");
              Thread.sleep(20_000);
            } catch (Exception e) {
              e.printStackTrace();
              System.exit(1);
            }
          }
        });
  }
}
