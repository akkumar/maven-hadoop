package com.github.hadoop.maven.plugin;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/**
 * Writes jars.
 * 
 * 
 */
public class JarWriter {

  /**
   * Given a root directory, this writes the contents of the same as a jar file.
   * The path to files inside the jar are relative paths, relative to the root
   * directory specified.
   * 
   * @param jarRootDir
   *          Root Directory that serves as an input to writing the jars.
   * @param os
   *          OutputStream to which the jar is to be packed
   * @throws FileNotFoundException
   * @throws IOException
   */
  public void packToJar(File jarRootDir, OutputStream os)
      throws FileNotFoundException, IOException {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");

    JarOutputStream target = new JarOutputStream(os, manifest);
    for (File nestedFile : jarRootDir.listFiles())
      add(jarRootDir.getPath().replace("\\", "/"), nestedFile, target);
    target.close();
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
