package athena.client


object ShapelessUtils {

  import shapeless._
  import scala.language.higherKinds

  trait Appender[H, T] {
    type Out <: HList
    def apply(h: H, t: T): Out
  }
  trait LowPriorityAppenders {
    //appends two scalars to create an HList
    implicit def scalar[H0, T0]: Appender.Aux[H0, T0, H0 :: T0 :: HNil] =
      new Appender[H0, T0] {
        type Out = H0 :: T0 :: HNil
        override def apply(h: H0, t: T0): Out = h :: t :: HNil
      }
  }
  object Appender extends LowPriorityAppenders {
    type Aux[H, T, Out0 <: HList] = Appender[H, T] { type Out = Out0 }

    implicit def prepender[H0, T0 <: HList]: Aux[H0, T0, H0 :: T0] =
      new Appender[H0, T0] {
        type Out = H0 :: T0
        override def apply(h: H0, t: T0): Out = h :: t
      }

    implicit def appender[H0 <: HList, T0, Out0 <: HList](implicit prepend: PrependAux[H0, T0 :: HNil, Out0]): Aux[H0, T0, Out0] =
      new Appender[H0, T0] {
        type Out = Out0
        override def apply(h: H0, t: T0): Out = prepend(h, t :: HNil)
      }

    implicit def hlistJoiner[H0 <: HList, T0 <: HList, Out0 <: HList](implicit prepend: PrependAux[H0, T0, Out0]): Aux[H0, T0, Out0] =
      new Appender[H0, T0] {
        type Out = Out0
        override def apply(h: H0, t: T0): Out = prepend(h, t)
      }
  }

  trait Reducer[M[_], H, T] {
    type Out <: HList
    def apply(mh: M[H], mt: M[T]): M[Out]
  }
  object Reducer {
    type Aux[M[_], H, T, Out0 <: HList] = Reducer[M, H, T] { type Out = Out0 }
  }


}
