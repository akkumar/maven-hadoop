/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
 * Pack the dependencies into an archive to be fed to the hadoop jar command.
 * <p>
 * Important: The list of dependencies in the hadoop CP are ignored / filtered
 * to avoid namespace collision.
 * <p>
 * Internal Workings:
 * <ul>
 * <li>To go hand in hand with the compile target, this copies the dependencies
 * to target/hadoop-deploy/lib directory.</li>
 * <li>The dependencies that are already present in $HADOOP_HOME/lib/*.jar , if
 * present in the project's dependencies are ignored.</li>
 * </ul>
 * <p>
 * <h3>Installation</h3> Install the plugin as follows.
 * 
 * <pre>
 * 
 *  &lt;plugin&gt; 
 *         &lt;groupId&gt;com.github.maven-hadoop.plugin&lt;/groupId&gt;
 *         &lt;artifactId&gt;maven-hadoop-plugin&lt;/artifactId&gt;
 *         &lt;version&gt;0.2.0&lt;/version&gt;
 *         &lt;configuration&gt;
 *           &lt;hadoopHome&gt;/opt/software/hadoop&lt;/hadoopHome&gt;
 *         &lt;/configuration&gt;
 *      &lt;/plugin&gt;
 * </pre>
 * 
 * <h3>Usage:</h3>
 * 
 * <code>
 * $ mvn compile hadoop:pack
 * </code>
 * 
 * @goal pack
 * 
 */
public class PackMojo extends AbstractMojo {

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
