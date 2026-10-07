package edu.course.bigdata.hdfs;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;

/**
 * 实验一 HDFS Java API综合程序。
 *
 * 运行示例：
 * hadoop jar build/experiment1.jar \
 *   edu.course.bigdata.hdfs.HdfsOperations cat /user/vboxuser/file.txt
 */
public final class HdfsOperations {
    private HdfsOperations() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0 || "help".equalsIgnoreCase(args[0])) {
            printHelp();
            return;
        }

        Configuration conf = new Configuration();
        try (FileSystem fs = FileSystem.get(conf)) {
            String command = args[0].toLowerCase();
            switch (command) {
                case "upload":
                    requireArgs(args, 3);
                    upload(fs, conf, new File(args[1]), new Path(args[2]));
                    break;
                case "download":
                    requireArgs(args, 3);
                    download(fs, new Path(args[1]), new File(args[2]));
                    break;
                case "cat":
                    requireArgs(args, 2);
                    cat(fs, conf, new Path(args[1]));
                    break;
                case "stat":
                    requireArgs(args, 2);
                    printStatus(fs, new Path(args[1]));
                    break;
                case "list":
                    requireArgs(args, 2);
                    listRecursively(fs, new Path(args[1]));
                    break;
                case "touch":
                    requireArgs(args, 2);
                    touch(fs, new Path(args[1]));
                    break;
                case "delete-file":
                    requireArgs(args, 2);
                    deleteFile(fs, new Path(args[1]));
                    break;
                case "mkdir":
                    requireArgs(args, 2);
                    mkdir(fs, new Path(args[1]));
                    break;
                case "rmdir":
                    requireArgs(args, 2);
                    removeEmptyDirectory(fs, new Path(args[1]));
                    break;
                case "append":
                    requireArgs(args, 4);
                    append(fs, conf, new Path(args[1]), args[2], joinFrom(args, 3));
                    break;
                case "move":
                    requireArgs(args, 3);
                    move(fs, new Path(args[1]), new Path(args[2]));
                    break;
                default:
                    System.err.println("未知命令：" + args[0]);
                    printHelp();
                    System.exit(2);
            }
        }
    }

    private static void upload(FileSystem fs, Configuration conf, File local, Path target)
            throws IOException {
        if (!local.isFile()) {
            throw new IOException("本地文件不存在：" + local.getAbsolutePath());
        }
        ensureParent(fs, target);

        boolean append = false;
        boolean overwrite = false;
        if (fs.exists(target)) {
            System.out.print("HDFS文件已存在。输入 a 追加、o 覆盖、c 取消：");
            Scanner scanner = new Scanner(System.in, "UTF-8");
            String choice = scanner.nextLine().trim().toLowerCase();
            if ("a".equals(choice)) {
                append = true;
            } else if ("o".equals(choice)) {
                overwrite = true;
            } else {
                System.out.println("已取消上传。原文件未改变。");
                return;
            }
        }

        try (BufferedInputStream input = new BufferedInputStream(new FileInputStream(local));
             FSDataOutputStream output = append ? fs.append(target) : fs.create(target, overwrite)) {
            IOUtils.copyBytes(input, output, conf, false);
        }
        System.out.println((append ? "追加" : "上传") + "完成：" + target);
    }

    private static void download(FileSystem fs, Path source, File requested) throws IOException {
        if (!fs.exists(source)) {
            throw new IOException("HDFS文件不存在：" + source);
        }

        File destination = requested;
        if (requested.isDirectory()) {
            destination = new File(requested, source.getName());
        }
        destination = nextAvailableFile(destination);

        File parent = destination.getAbsoluteFile().getParentFile();
        if (parent != null && !parent.exists() && !parent.mkdirs()) {
            throw new IOException("无法创建本地目录：" + parent);
        }

        fs.copyToLocalFile(false, source, new Path(destination.getAbsolutePath()), true);
        System.out.println("下载完成：" + destination.getAbsolutePath());
    }

    private static File nextAvailableFile(File requested) {
        if (!requested.exists()) {
            return requested;
        }
        String name = requested.getName();
        int dot = name.lastIndexOf('.');
        String base = dot > 0 ? name.substring(0, dot) : name;
        String extension = dot > 0 ? name.substring(dot) : "";
        File parent = requested.getAbsoluteFile().getParentFile();
        int index = 1;
        File candidate;
        do {
            candidate = new File(parent, base + "_" + index + extension);
            index++;
        } while (candidate.exists());
        return candidate;
    }

    private static void cat(FileSystem fs, Configuration conf, Path file) throws IOException {
        requireFile(fs, file);
        try (FSDataInputStream input = fs.open(file)) {
            IOUtils.copyBytes(input, System.out, conf, false);
        }
    }

    private static void printStatus(FileSystem fs, Path path) throws IOException {
        if (!fs.exists(path)) {
            throw new IOException("路径不存在：" + path);
        }
        FileStatus status = fs.getFileStatus(path);
        System.out.println("路径：" + status.getPath());
        System.out.println("类型：" + (status.isDirectory() ? "目录" : "文件"));
        System.out.println("权限：" + status.getPermission());
        System.out.println("所有者：" + status.getOwner());
        System.out.println("用户组：" + status.getGroup());
        System.out.println("大小：" + status.getLen() + " 字节");
        System.out.println("修改时间：" + formatTime(status.getModificationTime()));
        System.out.println("访问时间：" + formatTime(status.getAccessTime()));
        System.out.println("说明：HDFS FileStatus没有独立的文件创建时间字段。");
    }

    private static void listRecursively(FileSystem fs, Path directory) throws IOException {
        if (!fs.exists(directory)) {
            throw new IOException("目录不存在：" + directory);
        }
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(directory, true);
        System.out.printf("%-10s %-12s %-20s %s%n", "权限", "大小", "修改时间", "路径");
        while (iterator.hasNext()) {
            LocatedFileStatus status = iterator.next();
            System.out.printf("%-10s %-12d %-20s %s%n",
                    status.getPermission(), status.getLen(),
                    formatTime(status.getModificationTime()), status.getPath());
        }
    }

    private static void touch(FileSystem fs, Path file) throws IOException {
        ensureParent(fs, file);
        if (fs.exists(file)) {
            System.out.println("文件已存在，未覆盖：" + file);
            return;
        }
        try (FSDataOutputStream ignored = fs.create(file, false)) {
            // 创建空文件。
        }
        System.out.println("文件创建成功：" + file);
    }

    private static void deleteFile(FileSystem fs, Path file) throws IOException {
        requireFile(fs, file);
        if (!fs.delete(file, false)) {
            throw new IOException("文件删除失败：" + file);
        }
        System.out.println("文件删除成功：" + file);
    }

    private static void mkdir(FileSystem fs, Path directory) throws IOException {
        if (fs.mkdirs(directory)) {
            System.out.println("目录创建成功：" + directory);
        } else if (fs.exists(directory)) {
            System.out.println("目录已经存在：" + directory);
        } else {
            throw new IOException("目录创建失败：" + directory);
        }
    }

    private static void removeEmptyDirectory(FileSystem fs, Path directory) throws IOException {
        if (!fs.exists(directory)) {
            throw new IOException("目录不存在：" + directory);
        }
        FileStatus status = fs.getFileStatus(directory);
        if (!status.isDirectory()) {
            throw new IOException("指定路径不是目录：" + directory);
        }
        if (fs.listStatus(directory).length != 0) {
            System.out.println("目录非空，按实验要求不删除：" + directory);
            return;
        }
        if (!fs.delete(directory, false)) {
            throw new IOException("空目录删除失败：" + directory);
        }
        System.out.println("空目录删除成功：" + directory);
    }

    private static void append(FileSystem fs, Configuration conf, Path file,
                               String position, String content) throws IOException {
        requireFile(fs, file);
        byte[] bytes = (content + System.lineSeparator()).getBytes(StandardCharsets.UTF_8);
        if ("end".equalsIgnoreCase(position)) {
            try (FSDataOutputStream output = fs.append(file)) {
                output.write(bytes);
            }
            System.out.println("内容已追加到文件末尾：" + file);
        } else if ("begin".equalsIgnoreCase(position)) {
            prependSafely(fs, conf, file, bytes);
            System.out.println("内容已追加到文件开头：" + file);
        } else {
            throw new IllegalArgumentException("追加位置只能是 begin 或 end。");
        }
    }

    private static void prependSafely(FileSystem fs, Configuration conf, Path file, byte[] prefix)
            throws IOException {
        Path parent = file.getParent() == null ? new Path("/") : file.getParent();
        String suffix = String.valueOf(System.currentTimeMillis());
        Path temporary = new Path(parent, "." + file.getName() + ".prepend." + suffix);
        Path backup = new Path(parent, "." + file.getName() + ".backup." + suffix);

        try (FSDataOutputStream output = fs.create(temporary, false);
             FSDataInputStream input = fs.open(file)) {
            output.write(prefix);
            IOUtils.copyBytes(input, output, conf, false);
        }

        if (!fs.rename(file, backup)) {
            fs.delete(temporary, false);
            throw new IOException("无法创建原文件备份：" + backup);
        }
        if (!fs.rename(temporary, file)) {
            fs.rename(backup, file);
            throw new IOException("替换文件失败，已经尝试恢复原文件。");
        }
        fs.delete(backup, false);
    }

    private static void move(FileSystem fs, Path source, Path destination) throws IOException {
        if (!fs.exists(source)) {
            throw new IOException("源路径不存在：" + source);
        }
        if (fs.exists(destination)) {
            throw new IOException("目标路径已经存在，为避免覆盖已取消：" + destination);
        }
        ensureParent(fs, destination);
        if (!fs.rename(source, destination)) {
            throw new IOException("移动失败：" + source + " -> " + destination);
        }
        System.out.println("移动成功：" + source + " -> " + destination);
    }

    private static void ensureParent(FileSystem fs, Path path) throws IOException {
        Path parent = path.getParent();
        if (parent != null && !fs.exists(parent) && !fs.mkdirs(parent)) {
            throw new IOException("无法创建父目录：" + parent);
        }
    }

    private static void requireFile(FileSystem fs, Path file) throws IOException {
        if (!fs.exists(file)) {
            throw new IOException("文件不存在：" + file);
        }
        if (!fs.getFileStatus(file).isFile()) {
            throw new IOException("指定路径不是文件：" + file);
        }
    }

    private static void requireArgs(String[] args, int minimum) {
        if (args.length < minimum) {
            printHelp();
            throw new IllegalArgumentException("参数数量不足。");
        }
    }

    private static String joinFrom(String[] args, int start) {
        StringBuilder builder = new StringBuilder();
        for (int i = start; i < args.length; i++) {
            if (i > start) {
                builder.append(' ');
            }
            builder.append(args[i]);
        }
        return builder.toString();
    }

    private static String formatTime(long millis) {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(millis));
    }

    private static void printHelp() {
        System.out.println("用法：HdfsOperations <命令> <参数>");
        System.out.println("  upload <本地文件> <HDFS文件>          上传，存在时由用户选择追加或覆盖");
        System.out.println("  download <HDFS文件> <本地文件或目录>  下载，重名时自动改名");
        System.out.println("  cat <HDFS文件>                       输出文件内容");
        System.out.println("  stat <HDFS路径>                      显示权限、大小、时间和路径");
        System.out.println("  list <HDFS目录>                      递归显示目录中的全部文件");
        System.out.println("  touch <HDFS文件>                     自动创建父目录和空文件");
        System.out.println("  delete-file <HDFS文件>               删除指定文件");
        System.out.println("  mkdir <HDFS目录>                     递归创建目录");
        System.out.println("  rmdir <HDFS目录>                     仅删除空目录");
        System.out.println("  append <HDFS文件> <begin|end> <内容>  在文件开头或末尾追加内容");
        System.out.println("  move <HDFS源路径> <HDFS目标路径>      移动文件且不覆盖目标");
    }
}
