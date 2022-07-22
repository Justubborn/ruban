package cn.econta.ruban;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.cli.Argument;
import io.vertx.core.cli.CLI;
import io.vertx.core.cli.CommandLine;
import io.vertx.core.cli.Option;
import io.vertx.core.file.FileSystem;
import io.vertx.core.json.JsonObject;

import java.util.Arrays;

/**
 * @author Justubborn
 * @since 2022/2/28
 */
public class Main {

  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();
    FileSystem fs = vertx.fileSystem();
    JsonObject conf = fs.readFileBlocking("./conf.json").toJsonObject();
    CLI cli = CLI.create("copy")
      .setSummary("A command line interface to copy files.")
      .addOption(new Option()
        .setLongName("directory")
        .setShortName("R")
        .setDescription("enables directory support")
        .setFlag(true))
      .addArgument(new Argument()
        .setIndex(0)
        .setDescription("mainId")
        .setArgName("mainId"));
    CommandLine commandLine = cli.parse(Arrays.asList(args));
    DeploymentOptions options = new DeploymentOptions().setConfig(conf);
    vertx.deployVerticle(new MainVerticle(commandLine.getArgumentValue(0).toString()), options)
      .onFailure(System.out::println)
      .onComplete(event -> vertx.close());
  }

}
