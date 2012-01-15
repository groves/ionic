package ionic.util

object RunnableImplicit {
  implicit def funToRunnable(f: () => Unit) = new Runnable() { def run() = f() }
}
