package org.apache.hadoop.mojo;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.apache.commons.io.FileUtils;
import org.apache.maven.project.MavenProject;
import org.apache.maven.artifact.Artifact;
import org.apache.maven.model.Build;

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
   * The maven project.
   * 
   * @parameter expression="${project}"
   * @required
   * @readonly
   */
  protected MavenProject project;

  /**
   * @parameter expression="${project.build}"
   * @required
   * @readonly
   */
  protected Build build;

  /**
   * HBase Configuration properties
   * 
   * @parameter
   */
  private Properties hbaseConfiguration;

  /**
   * @parameter expression="${project.build.directory}/hadoop-deploy"
   * @readonly
   */
  protected File outputDirectory;

  /**
   * @parameter
   */
  private File hadoopHome;

  public void execute() throws MojoExecutionException {
    if (this.hadoopHome == null) {
      throw new MojoExecutionException(
          "hadoopHome property needs to be set for the plugin to work");
    }
    try {
      File jarRootDir = createHadoopDeployArtifacts();
      File jarName = packToJar(jarRootDir);
      getLog().info("Hadoop  job jar file available at " + jarName);
    } catch (IOException e) {
      throw new IllegalStateException("Error creating output directory", e);
    }

  }

  /**
   * Create the hadoop deploy artifacts
   * 
   * @throws IOException
   * @return File that contains the root of jar file to be packed.
   */
  private File createHadoopDeployArtifacts() throws IOException {
    FileUtils.deleteDirectory(outputDirectory);
    File rootDir = new File(outputDirectory.getAbsolutePath() + File.separator
        + "root");
    FileUtils.forceMkdir(rootDir);

    File jarlibdir = new File(rootDir.getAbsolutePath() + File.separator
        + "lib");
    FileUtils.forceMkdir(jarlibdir);

    File classesdir = new File(project.getBuild().getDirectory()
        + File.separator + "classes");
    FileUtils.copyDirectory(classesdir, rootDir);
    List<File> dependencies = this.getProjectDependencies();
    List<File> filteredDependencies = this.filterDependencies(dependencies);
    getLog().info(
        "Dependencies of this project independent of hadoop classpath "
            + filteredDependencies);
    for (File dependency : filteredDependencies) {
      FileUtils.copyFileToDirectory(dependency, jarlibdir);
    }
    return rootDir;
  }

  /**
   * Filter hadoop dependencies from the classpath.
   * 
   * @param dependencies
   * @return
   */
  private List<File> filterDependencies(List<File> dependencies) {
    List<String> hadoopDependencies = getHadoopDependencies();
    List<File> output = new ArrayList<File>();
    for (final File inputDependency : dependencies) {
      final String name = inputDependency.getName();
      if (name.startsWith("hadoop") || hadoopDependencies.contains(name)) {
        continue; // skip other dependencies in hadoop cp as well.
      } else {
        output.add(inputDependency);
      }
    }
    return output;

  }

  /**
   * Retrieve the list of hadoop dependencies since we want to perform a set
   * operation of A - B before packing our jar.
   * 
   * @return
   */
  private List<String> getHadoopDependencies() {
    File hadoopLib = new File(this.hadoopHome.getAbsoluteFile()
        + File.separator + "lib");
    Collection<File> hadoopDependencies = FileUtils.listFiles(hadoopLib,
        new String[] { "jar" }, true);
    List<String> outputJars = new ArrayList<String>();
    for (final File hadoopDependency : hadoopDependencies) {
      outputJars.add(hadoopDependency.getName());
    }
    return outputJars;
  }

  /**
   * Retrieve the project dependencies.
   * 
   * @return
   */
  @SuppressWarnings("unchecked")
  private List<File> getProjectDependencies() {
    List<File> jarDependencies = new ArrayList<File>();
    final Collection<Artifact> artifacts = project.getArtifacts();
    for (Artifact artifact : artifacts) {
      if ("jar".equals(artifact.getType())) {
        File file = artifact.getFile();
        if (file != null && file.exists()) {
          jarDependencies.add(file);
        } else {
          getLog().warn("Dependency file not found: " + artifact);
        }
      }
    }
    return jarDependencies;
  }

  private File packToJar(File jarRootDir) throws FileNotFoundException,
      IOException {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    File jarName = new File(this.outputDirectory.getAbsolutePath()
        + File.separator + this.project.getArtifactId() + "-hdeploy.jar");
    JarOutputStream target = new JarOutputStream(new FileOutputStream(jarName),
        manifest);
    for (File nestedFile : jarRootDir.listFiles())
      add(jarRootDir.getPath().replace("\\", "/"), nestedFile, target);
    target.close();
    return jarName;
  }

  private void add(String prefix, File source, JarOutputStream target)
      throws IOException {
    BufferedInputStream in = null;
    try {
      if (source.isDirectory()) {
        String name = source.getPath().replace("\\", "/");
        if (!name.isEmpty()) {
          if (!name.endsWith("/"))
            name += "/";
          JarEntry entry = new JarEntry(name.substring(prefix.length() + 1));
          entry.setTime(source.lastModified());
          target.putNextEntry(entry);
          target.closeEntry();
        }
        for (File nestedFile : source.listFiles())
          add(prefix, nestedFile, target);
        return;
      }

      String jarentryName = source.getPath().replace("\\", "/").substring(
          prefix.length() + 1);
      JarEntry entry = new JarEntry(jarentryName);
      entry.setTime(source.lastModified());
      target.putNextEntry(entry);
      in = new BufferedInputStream(new FileInputStream(source));

      byte[] buffer = new byte[1024];
      while (true) {
        int count = in.read(buffer);
        if (count == -1)
          break;
        target.write(buffer, 0, count);
      }
      target.closeEntry();
    } finally {
      if (in != null)
        in.close();
    }
  }

}
