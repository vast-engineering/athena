package athena.data

trait ValueReader[A] {
  def read(cv: CValue): A
}

trait ValueWriter[A] {
  def write(a: A): CValue
}



