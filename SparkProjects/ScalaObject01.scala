object ScalaObject01 {
// In scala Object is a Singleton Class and Main function must be a part of it.

  def main(args: Array[String]): Unit = {
    println("Hello ScalaObject01")

    println(args(0))
  }
}

// From the menu, Run this object (This is possible for Main class containing objects, to directly run)
// Command line Run time arguments is also possible from IntelliJ. Edit Configurations -> Program arguments.