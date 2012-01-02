package ionic.util

import react.Slot
import react.UnitSlot

object ReactImplicits {
  implicit def func0ToUnitSlot(f: () => _) = new UnitSlot() { def onEmit() = f() }
  implicit def func0ToSlot[T](f: () => _) = new Slot[T]() { def onEmit(t: T) = f() }
  implicit def func1ToSlot[T](f: T => _) = new Slot[T]() { def onEmit(t: T) = f(t) }
}
