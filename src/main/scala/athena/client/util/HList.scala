package athena.client
package util

sealed trait HList extends Product

final case class ::[+H, +T <: HList](head : H, tail : T) extends HList {
  override def toString = head+" :: "+tail.toString
}

sealed trait HNil extends HList {
  def ::[H](h : H) = util.::(h, this)
  override def toString = "HNil"
}
case object HNil extends HNil

object HList {
  implicit class HListOps[L <: HList](val l: L) extends AnyVal {
    /**
     * Prepend the argument element to this `HList`.
     */
    def ::[H](h : H) : H :: L = util.::(h, l)
  }
}

trait HLister[-T <: Product] {
  type Out <: HList
  def apply(t: T): Out
}
object HLister {
  type Aux[-T <: Product, Out0] = HLister[T] {type Out = Out0}

  implicit def tupleHLister1[A]: Aux[Product1[A], A :: HNil] =
    new HLister[Product1[A]] {
      type Out = A :: HNil
      def apply(t: Product1[A]) = t._1 :: HNil
    }

  implicit def tupleHLister2[A, B]: Aux[Product2[A, B], A :: B :: HNil] =
    new HLister[Product2[A, B]] {
      type Out = A :: B :: HNil
      def apply(t: Product2[A, B]) = t._1 :: t._2 :: HNil
    }

  implicit def tupleHLister3[A, B, C]: Aux[Product3[A, B, C], A :: B :: C :: HNil] =
    new HLister[Product3[A, B, C]] {
      type Out = A :: B :: C :: HNil
      def apply(t: Product3[A, B, C]) = t._1 :: t._2 :: t._3 :: HNil
    }

  implicit def tupleHLister4[A, B, C, D]: Aux[Product4[A, B, C, D], A :: B :: C :: D :: HNil] =
    new HLister[Product4[A, B, C, D]] {
      type Out = A :: B :: C :: D :: HNil
      def apply(t: Product4[A, B, C, D]) = t._1 :: t._2 :: t._3 :: t._4 :: HNil
    }

  implicit def tupleHLister5[A, B, C, D, E]: Aux[Product5[A, B, C, D, E], A :: B :: C :: D :: E :: HNil] =
    new HLister[Product5[A, B, C, D, E]] {
      type Out = A :: B :: C :: D :: E :: HNil
      def apply(t: Product5[A, B, C, D, E]) = t._1 :: t._2 :: t._3 :: t._4 :: t._5 :: HNil
    }

  implicit def tupleHLister6[A, B, C, D, E, F]: Aux[Product6[A, B, C, D, E, F], A :: B :: C :: D :: E :: F :: HNil] =
    new HLister[Product6[A, B, C, D, E, F]] {
      type Out = A :: B :: C :: D :: E :: F :: HNil
      def apply(t: Product6[A, B, C, D, E, F]) = t._1 :: t._2 :: t._3 :: t._4 :: t._5 :: t._6 :: HNil
    }

  implicit def tupleHLister7[A, B, C, D, E, F, G]: Aux[Product7[A, B, C, D, E, F, G], A :: B :: C :: D :: E :: F :: G :: HNil] =
    new HLister[Product7[A, B, C, D, E, F, G]] {
      type Out = A :: B :: C :: D :: E :: F :: G :: HNil
      def apply(t: Product7[A, B, C, D, E, F, G]) = t._1 :: t._2 :: t._3 :: t._4 :: t._5 :: t._6 :: t._7 :: HNil
    }

  implicit def tupleHLister8[A, B, C, D, E, F, G, H]: Aux[Product8[A, B, C, D, E, F, G, H], A :: B :: C :: D :: E :: F :: G :: H :: HNil] =
    new HLister[Product8[A, B, C, D, E, F, G, H]] {
      type Out = A :: B :: C :: D :: E :: F :: G :: H :: HNil
      def apply(t: Product8[A, B, C, D, E, F, G, H]) = t._1 :: t._2 :: t._3 :: t._4 :: t._5 :: t._6 :: t._7 :: t._8 :: HNil
    }

  implicit def tupleHLister9[A, B, C, D, E, F, G, H, I]: Aux[Product9[A, B, C, D, E, F, G, H, I], A :: B :: C :: D :: E :: F :: G :: H :: I :: HNil] =
    new HLister[Product9[A, B, C, D, E, F, G, H, I]] {
      type Out = A :: B :: C :: D :: E :: F :: G :: H :: I :: HNil
      def apply(t: Product9[A, B, C, D, E, F, G, H, I]) = t._1 :: t._2 :: t._3 :: t._4 :: t._5 :: t._6 :: t._7 :: t._8 :: t._9 :: HNil
    }

  implicit def tupleHLister10[A, B, C, D, E, F, G, H, I, J]: Aux[Product10[A, B, C, D, E, F, G, H, I, J], A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: HNil] =
    new HLister[Product10[A, B, C, D, E, F, G, H, I, J]] {
      type Out = A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: HNil
      def apply(t: Product10[A, B, C, D, E, F, G, H, I, J]) = t._1 :: t._2 :: t._3 :: t._4 :: t._5 :: t._6 :: t._7 :: t._8 :: t._9 :: t._10 :: HNil
    }

  implicit def tupleHLister11[A, B, C, D, E, F, G, H, I, J, K]: Aux[Product11[A, B, C, D, E, F, G, H, I, J, K], A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: HNil] =
    new HLister[Product11[A, B, C, D, E, F, G, H, I, J, K]] {
      type Out = A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: HNil
      def apply(t: Product11[A, B, C, D, E, F, G, H, I, J, K]) = t._1 :: t._2 :: t._3 :: t._4 :: t._5 :: t._6 :: t._7 :: t._8 :: t._9 :: t._10 :: t._11 :: HNil
    }

  implicit def tupleHLister12[A, B, C, D, E, F, G, H, I, J, K, L]: Aux[Product12[A, B, C, D, E, F, G, H, I, J, K, L], A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: HNil] =
    new HLister[Product12[A, B, C, D, E, F, G, H, I, J, K, L]] {
      type Out = A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: HNil
      def apply(t: Product12[A, B, C, D, E, F, G, H, I, J, K, L]) = t._1 :: t._2 :: t._3 :: t._4 :: t._5 :: t._6 :: t._7 :: t._8 :: t._9 :: t._10 :: t._11 :: t._12 :: HNil
    }

  implicit def tupleHLister13[A, B, C, D, E, F, G, H, I, J, K, L, M]: Aux[Product13[A, B, C, D, E, F, G, H, I, J, K, L, M], A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: HNil] =
    new HLister[Product13[A, B, C, D, E, F, G, H, I, J, K, L, M]] {
      type Out = A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: HNil
      def apply(t: Product13[A, B, C, D, E, F, G, H, I, J, K, L, M]) = t._1 :: t._2 :: t._3 :: t._4 :: t._5 :: t._6 :: t._7 :: t._8 :: t._9 :: t._10 :: t._11 :: t._12 :: t._13 :: HNil
    }

  implicit def tupleHLister14[A, B, C, D, E, F, G, H, I, J, K, L, M, N]: Aux[Product14[A, B, C, D, E, F, G, H, I, J, K, L, M, N], A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: HNil] =
    new HLister[Product14[A, B, C, D, E, F, G, H, I, J, K, L, M, N]] {
      type Out = A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: HNil
      def apply(t: Product14[A, B, C, D, E, F, G, H, I, J, K, L, M, N]) = t._1 :: t._2 :: t._3 :: t._4 :: t._5 :: t._6 :: t._7 :: t._8 :: t._9 :: t._10 :: t._11 :: t._12 :: t._13 :: t._14 :: HNil
    }

  implicit def tupleHLister15[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O]: Aux[Product15[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O], A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: HNil] =
    new HLister[Product15[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O]] {
      type Out = A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: HNil
      def apply(t: Product15[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O]) = t._1 :: t._2 :: t._3 :: t._4 :: t._5 :: t._6 :: t._7 :: t._8 :: t._9 :: t._10 :: t._11 :: t._12 :: t._13 :: t._14 :: t._15 :: HNil
    }

  implicit def tupleHLister16[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P]: Aux[Product16[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P], A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: HNil] =
    new HLister[Product16[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P]] {
      type Out = A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: HNil
      def apply(t: Product16[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P]) = t._1 :: t._2 :: t._3 :: t._4 :: t._5 :: t._6 :: t._7 :: t._8 :: t._9 :: t._10 :: t._11 :: t._12 :: t._13 :: t._14 :: t._15 :: t._16 :: HNil
    }

  implicit def tupleHLister17[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q]: Aux[Product17[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q], A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: HNil] =
    new HLister[Product17[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q]] {
      type Out = A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: HNil
      def apply(t: Product17[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q]) = t._1 :: t._2 :: t._3 :: t._4 :: t._5 :: t._6 :: t._7 :: t._8 :: t._9 :: t._10 :: t._11 :: t._12 :: t._13 :: t._14 :: t._15 :: t._16 :: t._17 :: HNil
    }

  implicit def tupleHLister18[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R]: Aux[Product18[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R], A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: HNil] =
    new HLister[Product18[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R]] {
      type Out = A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: HNil
      def apply(t: Product18[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R]) = t._1 :: t._2 :: t._3 :: t._4 :: t._5 :: t._6 :: t._7 :: t._8 :: t._9 :: t._10 :: t._11 :: t._12 :: t._13 :: t._14 :: t._15 :: t._16 :: t._17 :: t._18 :: HNil
    }

  implicit def tupleHLister19[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S]: Aux[Product19[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S], A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: S :: HNil] =
    new HLister[Product19[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S]] {
      type Out = A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: S :: HNil
      def apply(t: Product19[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S]) = t._1 :: t._2 :: t._3 :: t._4 :: t._5 :: t._6 :: t._7 :: t._8 :: t._9 :: t._10 :: t._11 :: t._12 :: t._13 :: t._14 :: t._15 :: t._16 :: t._17 :: t._18 :: t._19 :: HNil
    }

  implicit def tupleHLister20[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T]: Aux[Product20[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T], A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: S :: T :: HNil] =
    new HLister[Product20[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T]] {
      type Out = A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: S :: T :: HNil
      def apply(t: Product20[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T]) = t._1 :: t._2 :: t._3 :: t._4 :: t._5 :: t._6 :: t._7 :: t._8 :: t._9 :: t._10 :: t._11 :: t._12 :: t._13 :: t._14 :: t._15 :: t._16 :: t._17 :: t._18 :: t._19 :: t._20 :: HNil
    }

  implicit def tupleHLister21[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U]: Aux[Product21[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U], A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: S :: T :: U :: HNil] =
    new HLister[Product21[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U]] {
      type Out = A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: S :: T :: U :: HNil
      def apply(t: Product21[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U]) = t._1 :: t._2 :: t._3 :: t._4 :: t._5 :: t._6 :: t._7 :: t._8 :: t._9 :: t._10 :: t._11 :: t._12 :: t._13 :: t._14 :: t._15 :: t._16 :: t._17 :: t._18 :: t._19 :: t._20 :: t._21 :: HNil
    }

  implicit def tupleHLister22[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V]: Aux[Product22[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V], A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: S :: T :: U :: V :: HNil] =
    new HLister[Product22[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V]] {
      type Out = A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: S :: T :: U :: V :: HNil
      def apply(t: Product22[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V]) = t._1 :: t._2 :: t._3 :: t._4 :: t._5 :: t._6 :: t._7 :: t._8 :: t._9 :: t._10 :: t._11 :: t._12 :: t._13 :: t._14 :: t._15 :: t._16 :: t._17 :: t._18 :: t._19 :: t._20 :: t._21 :: t._22 :: HNil
    }


}
