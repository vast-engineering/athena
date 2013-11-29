package athena.util

import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec

//stolen from spray

private[athena] trait ObjectRegistry[K, V <: AnyRef] {
  private[this] val _registry = new AtomicReference(Map.empty[K, V])

  @tailrec
  protected final def register(key: K, obj: V): obj.type = {
    val reg = registry
    val updated = reg.updated(key, obj)
    if (_registry.compareAndSet(reg, updated)) obj
    else register(key, obj)
  }

  protected def registry: Map[K, V] = _registry.get

  def getForKey(key: K): Option[V] = {
    val foo = registry.get(key)
    foo
  }
}
