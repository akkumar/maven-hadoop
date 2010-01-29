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

  public void execute() throws MojoExecutionException {
    getLog().info("Hello, world.");
  }
}
