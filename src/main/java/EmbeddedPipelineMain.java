import org.apache.flink.cdc.cli.CliFrontend;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class EmbeddedPipelineMain {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("Usage: EmbeddedPipelineMain <pipeline.yaml> [other args...]");
            System.exit(1);
        }
        String yamlName = args[0];

        try (InputStream in = EmbeddedPipelineMain.class.getClassLoader().getResourceAsStream(yamlName)) {
            if (in == null) {
                // classpath 里找不到，当作文件系统路径直接传给 CliFrontend
                CliFrontend.main(args);
                return;
            }

            // 解压到临时文件
            Path tmp = Files.createTempFile("flink-cdc-", ".yaml");
            tmp.toFile().deleteOnExit();
            try (OutputStream out = Files.newOutputStream(tmp)) {
                in.transferTo(out);
            }

            // 替换第一个参数为临时文件路径，其余参数原样保留
            String[] newArgs = new String[args.length];
            newArgs[0] = tmp.toAbsolutePath().toString();
            if (args.length > 1) {
                System.arraycopy(args, 1, newArgs, 1, args.length - 1);
            }

            CliFrontend.main(newArgs);
        }
    }
}