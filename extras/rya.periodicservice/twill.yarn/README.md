Since Rya has a dependency that is incompatible with Twill (Twill requires Guava 13.0.1), there are two options for packaging Rya to avoid dependency conflicts.

1) Package your application with the maven-bundle-plugin so dependencies are stored as jars in a lib directory inside the jar (similar to a mapreduce jar).  Then use the twill-ext project and `BundledJarRunner` and `BundledJarRunnable` to run a main class in a jar that has been packaged with its dependencies on the lib directory.

2) Package your application with the maven-shade-plugin and relocate [1] any conflicting dependencies.  Note, this will modify the bytecode of the classes.  Theoretically this approach will allow your application a tighter integration with Twill which can be a positive and negative.  Recommend you limit Twill dependencies to a single project and store all integration code there.

[1] https://maven.apache.org/plugins/maven-shade-plugin/examples/class-relocation.html