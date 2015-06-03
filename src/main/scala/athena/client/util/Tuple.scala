package athena.client.util

/**
 * Phantom type providing implicit evidence that a given type is a Tuple or Unit.
 */
sealed trait Tuple[T]

object Tuple {
  /**
   * Used to provide "is-Tuple" evidence where we know that a given value must be a tuple.
   */
  def yes[T]: Tuple[T] = null

  implicit def forNothing[A]: Tuple[Nothing] = null
  implicit def forUnit[A]: Tuple[Unit] = null
  implicit def forTuple1[A]: Tuple[Tuple1[A]] = null
  implicit def forTuple2[A, B]: Tuple[(A, B)] = null
  implicit def forTuple3[A, B, C]: Tuple[(A, B, C)] = null
  implicit def forTuple4[A, B, C, D]: Tuple[(A, B, C, D)] = null
  implicit def forTuple5[A, B, C, D, E]: Tuple[(A, B, C, D, E)] = null
  implicit def forTuple6[A, B, C, D, E, F]: Tuple[(A, B, C, D, E, F)] = null
  implicit def forTuple7[A, B, C, D, E, F, G]: Tuple[(A, B, C, D, E, F, G)] = null
  implicit def forTuple8[A, B, C, D, E, F, G, H]: Tuple[(A, B, C, D, E, F, G, H)] = null
  implicit def forTuple9[A, B, C, D, E, F, G, H, I]: Tuple[(A, B, C, D, E, F, G, H, I)] = null
  implicit def forTuple10[A, B, C, D, E, F, G, H, I, J]: Tuple[(A, B, C, D, E, F, G, H, I, J)] = null
  implicit def forTuple11[A, B, C, D, E, F, G, H, I, J, K]: Tuple[(A, B, C, D, E, F, G, H, I, J, K)] = null
  implicit def forTuple12[A, B, C, D, E, F, G, H, I, J, K, L]: Tuple[(A, B, C, D, E, F, G, H, I, J, K, L)] = null
  implicit def forTuple13[A, B, C, D, E, F, G, H, I, J, K, L, M]: Tuple[(A, B, C, D, E, F, G, H, I, J, K, L, M)] = null
  implicit def forTuple14[A, B, C, D, E, F, G, H, I, J, K, L, M, N]: Tuple[(A, B, C, D, E, F, G, H, I, J, K, L, M, N)] = null
  implicit def forTuple15[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O]: Tuple[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)] = null
  implicit def forTuple16[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P]: Tuple[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P)] = null
  implicit def forTuple17[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q]: Tuple[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q)] = null
  implicit def forTuple18[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R]: Tuple[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R)] = null
  implicit def forTuple19[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S]: Tuple[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S)] = null
  implicit def forTuple20[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T]: Tuple[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)] = null
  implicit def forTuple21[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U]: Tuple[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)] = null
  implicit def forTuple22[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V]: Tuple[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)] = null
}

trait Tupler[L <: HList] {
  type Out <: Product
  def apply(l : L) : Out
}
object Tupler {
  type Aux[L <: HList, Out0] = Tupler[L] { type Out = Out0 }

  implicit def hlistTupler1[A]: Aux[A :: HNil, Tuple1[A]] =
    new Tupler[A :: HNil] {
      type Out = Tuple1[A]
      def apply(hlist: A :: HNil) = hlist match { case a :: HNil => Tuple1(a) }
    }

  implicit def hlistTupler2[A, B]: Aux[A :: B :: HNil, (A, B)] =
    new Tupler[A :: B :: HNil] {
      type Out = (A, B)
      def apply(hlist: A :: B :: HNil) = hlist match { case a :: b :: HNil => (a, b) }
    }

  implicit def hlistTupler3[A, B, C]: Aux[A :: B :: C :: HNil, (A, B, C)] =
    new Tupler[A :: B :: C :: HNil] {
      type Out = (A, B, C)
      def apply(hlist: A :: B :: C :: HNil) = hlist match { case a :: b :: c :: HNil => (a, b, c) }
    }

  implicit def hlistTupler4[A, B, C, D]: Aux[A :: B :: C :: D :: HNil, (A, B, C, D)] =
    new Tupler[A :: B :: C :: D :: HNil] {
      type Out = (A, B, C, D)
      def apply(hlist: A :: B :: C :: D :: HNil) = hlist match { case a :: b :: c :: d :: HNil => (a, b, c, d) }
    }

  implicit def hlistTupler5[A, B, C, D, E]: Aux[A :: B :: C :: D :: E :: HNil, (A, B, C, D, E)] =
    new Tupler[A :: B :: C :: D :: E :: HNil] {
      type Out = (A, B, C, D, E)
      def apply(hlist: A :: B :: C :: D :: E :: HNil) = hlist match { case a :: b :: c :: d :: e :: HNil => (a, b, c, d, e) }
    }

  implicit def hlistTupler6[A, B, C, D, E, F]: Aux[A :: B :: C :: D :: E :: F :: HNil, (A, B, C, D, E, F)] =
    new Tupler[A :: B :: C :: D :: E :: F :: HNil] {
      type Out = (A, B, C, D, E, F)
      def apply(hlist: A :: B :: C :: D :: E :: F :: HNil) = hlist match { case a :: b :: c :: d :: e :: f :: HNil => (a, b, c, d, e, f) }
    }

  implicit def hlistTupler7[A, B, C, D, E, F, G]: Aux[A :: B :: C :: D :: E :: F :: G :: HNil, (A, B, C, D, E, F, G)] =
    new Tupler[A :: B :: C :: D :: E :: F :: G :: HNil] {
      type Out = (A, B, C, D, E, F, G)
      def apply(hlist: A :: B :: C :: D :: E :: F :: G :: HNil) = hlist match { case a :: b :: c :: d :: e :: f :: g :: HNil => (a, b, c, d, e, f, g) }
    }

  implicit def hlistTupler8[A, B, C, D, E, F, G, H]: Aux[A :: B :: C :: D :: E :: F :: G :: H :: HNil, (A, B, C, D, E, F, G, H)] =
    new Tupler[A :: B :: C :: D :: E :: F :: G :: H :: HNil] {
      type Out = (A, B, C, D, E, F, G, H)
      def apply(hlist: A :: B :: C :: D :: E :: F :: G :: H :: HNil) = hlist match { case a :: b :: c :: d :: e :: f :: g :: h :: HNil => (a, b, c, d, e, f, g, h) }
    }

  implicit def hlistTupler9[A, B, C, D, E, F, G, H, I]: Aux[A :: B :: C :: D :: E :: F :: G :: H :: I :: HNil, (A, B, C, D, E, F, G, H, I)] =
    new Tupler[A :: B :: C :: D :: E :: F :: G :: H :: I :: HNil] {
      type Out = (A, B, C, D, E, F, G, H, I)
      def apply(hlist: A :: B :: C :: D :: E :: F :: G :: H :: I :: HNil) = hlist match { case a :: b :: c :: d :: e :: f :: g :: h :: i :: HNil => (a, b, c, d, e, f, g, h, i) }
    }

  implicit def hlistTupler10[A, B, C, D, E, F, G, H, I, J]: Aux[A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: HNil, (A, B, C, D, E, F, G, H, I, J)] =
    new Tupler[A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: HNil] {
      type Out = (A, B, C, D, E, F, G, H, I, J)
      def apply(hlist: A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: HNil) = hlist match { case a :: b :: c :: d :: e :: f :: g :: h :: i :: j :: HNil => (a, b, c, d, e, f, g, h, i, j) }
    }

  implicit def hlistTupler11[A, B, C, D, E, F, G, H, I, J, K]: Aux[A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: HNil, (A, B, C, D, E, F, G, H, I, J, K)] =
    new Tupler[A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: HNil] {
      type Out = (A, B, C, D, E, F, G, H, I, J, K)
      def apply(hlist: A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: HNil) = hlist match { case a :: b :: c :: d :: e :: f :: g :: h :: i :: j :: k :: HNil => (a, b, c, d, e, f, g, h, i, j, k) }
    }

  implicit def hlistTupler12[A, B, C, D, E, F, G, H, I, J, K, L]: Aux[A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: HNil, (A, B, C, D, E, F, G, H, I, J, K, L)] =
    new Tupler[A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: HNil] {
      type Out = (A, B, C, D, E, F, G, H, I, J, K, L)
      def apply(hlist: A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: HNil) = hlist match { case a :: b :: c :: d :: e :: f :: g :: h :: i :: j :: k :: l :: HNil => (a, b, c, d, e, f, g, h, i, j, k, l) }
    }

  implicit def hlistTupler13[A, B, C, D, E, F, G, H, I, J, K, L, M]: Aux[A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: HNil, (A, B, C, D, E, F, G, H, I, J, K, L, M)] =
    new Tupler[A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: HNil] {
      type Out = (A, B, C, D, E, F, G, H, I, J, K, L, M)
      def apply(hlist: A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: HNil) = hlist match { case a :: b :: c :: d :: e :: f :: g :: h :: i :: j :: k :: l :: m :: HNil => (a, b, c, d, e, f, g, h, i, j, k, l, m) }
    }

  implicit def hlistTupler14[A, B, C, D, E, F, G, H, I, J, K, L, M, N]: Aux[A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: HNil, (A, B, C, D, E, F, G, H, I, J, K, L, M, N)] =
    new Tupler[A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: HNil] {
      type Out = (A, B, C, D, E, F, G, H, I, J, K, L, M, N)
      def apply(hlist: A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: HNil) = hlist match { case a :: b :: c :: d :: e :: f :: g :: h :: i :: j :: k :: l :: m :: n :: HNil => (a, b, c, d, e, f, g, h, i, j, k, l, m, n) }
    }

  implicit def hlistTupler15[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O]: Aux[A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: HNil, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)] =
    new Tupler[A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: HNil] {
      type Out = (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)
      def apply(hlist: A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: HNil) = hlist match { case a :: b :: c :: d :: e :: f :: g :: h :: i :: j :: k :: l :: m :: n :: o :: HNil => (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o) }
    }

  implicit def hlistTupler16[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P]: Aux[A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: HNil, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P)] =
    new Tupler[A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: HNil] {
      type Out = (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P)
      def apply(hlist: A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: HNil) = hlist match { case a :: b :: c :: d :: e :: f :: g :: h :: i :: j :: k :: l :: m :: n :: o :: p :: HNil => (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p) }
    }

  implicit def hlistTupler17[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q]: Aux[A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: HNil, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q)] =
    new Tupler[A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: HNil] {
      type Out = (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q)
      def apply(hlist: A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: HNil) = hlist match { case a :: b :: c :: d :: e :: f :: g :: h :: i :: j :: k :: l :: m :: n :: o :: p :: q :: HNil => (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q) }
    }

  implicit def hlistTupler18[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R]: Aux[A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: HNil, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R)] =
    new Tupler[A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: HNil] {
      type Out = (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R)
      def apply(hlist: A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: HNil) = hlist match { case a :: b :: c :: d :: e :: f :: g :: h :: i :: j :: k :: l :: m :: n :: o :: p :: q :: r :: HNil => (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r) }
    }

  implicit def hlistTupler19[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S]: Aux[A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: S :: HNil, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S)] =
    new Tupler[A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: S :: HNil] {
      type Out = (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S)
      def apply(hlist: A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: S :: HNil) = hlist match { case a :: b :: c :: d :: e :: f :: g :: h :: i :: j :: k :: l :: m :: n :: o :: p :: q :: r :: s :: HNil => (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s) }
    }

  implicit def hlistTupler20[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T]: Aux[A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: S :: T :: HNil, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)] =
    new Tupler[A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: S :: T :: HNil] {
      type Out = (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)
      def apply(hlist: A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: S :: T :: HNil) = hlist match { case a :: b :: c :: d :: e :: f :: g :: h :: i :: j :: k :: l :: m :: n :: o :: p :: q :: r :: s :: t :: HNil => (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t) }
    }

  implicit def hlistTupler21[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U]: Aux[A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: S :: T :: U :: HNil, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)] =
    new Tupler[A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: S :: T :: U :: HNil] {
      type Out = (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)
      def apply(hlist: A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: S :: T :: U :: HNil) = hlist match { case a :: b :: c :: d :: e :: f :: g :: h :: i :: j :: k :: l :: m :: n :: o :: p :: q :: r :: s :: t :: u :: HNil => (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u) }
    }

  implicit def hlistTupler22[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V]: Aux[A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: S :: T :: U :: V :: HNil, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)] =
    new Tupler[A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: S :: T :: U :: V :: HNil] {
      type Out = (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)
      def apply(hlist: A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: S :: T :: U :: V :: HNil) = hlist match { case a :: b :: c :: d :: e :: f :: g :: h :: i :: j :: k :: l :: m :: n :: o :: p :: q :: r :: s :: t :: u :: v :: HNil => (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v) }
    }

}
