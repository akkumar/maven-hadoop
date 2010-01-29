package org.apache.hadoop.mojo;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;

/**
 * Deploy the hadoop class
 * 
 * @goal deploy
 * 
 */
public class DeployMojo extends AbstractMojo {

  /**
   * The greeting to display.
   * 
   * @parameter expression="${deploy.toolrunner}"
   **/
  private String toolrunner;

  public void execute() throws MojoExecutionException {
    getLog().info("About to execute toolrunner" + toolrunner);
    try {
      Class<?> toolClass = Class.forName(toolrunner);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(
          "ToolRunner class not found in classpath", e);
    }

  }
}
