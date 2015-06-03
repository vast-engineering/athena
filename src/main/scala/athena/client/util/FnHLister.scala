package athena.client.util

trait FnHLister[F] {
  type Out
  def apply(t: F): Out
}

object FnHLister {
  type Aux[F, Out0] = FnHLister[F] { type Out = Out0 }

  implicit def fnHLister0[Res]: Aux[() => Res, (HNil) => Res] =
    new FnHLister[() => Res] {
      type Out = (HNil) => Res
      def apply(fn: () => Res) = (l: HNil) => fn()
    }

  implicit def fnHLister1[A, Res]: Aux[(A) => Res, (A :: HNil) => Res] =
    new FnHLister[(A) => Res] {
      type Out = (A :: HNil) => Res
      def apply(fn: (A) => Res) = { case a :: HNil => fn(a) }
    }

  implicit def fnHLister2[A, B, Res]: Aux[(A, B) => Res, (A :: B :: HNil) => Res] =
    new FnHLister[(A, B) => Res] {
      type Out = (A :: B :: HNil) => Res
      def apply(fn: (A, B) => Res) = { case a :: b :: HNil => fn(a, b) }
    }

  implicit def fnHLister3[A, B, C, Res]: Aux[(A, B, C) => Res, (A :: B :: C :: HNil) => Res] =
    new FnHLister[(A, B, C) => Res] {
      type Out = (A :: B :: C :: HNil) => Res
      def apply(fn: (A, B, C) => Res) = { case a :: b :: c :: HNil => fn(a, b, c) }
    }

  implicit def fnHLister4[A, B, C, D, Res]: Aux[(A, B, C, D) => Res, (A :: B :: C :: D :: HNil) => Res] =
    new FnHLister[(A, B, C, D) => Res] {
      type Out = (A :: B :: C :: D :: HNil) => Res
      def apply(fn: (A, B, C, D) => Res) = { case a :: b :: c :: d :: HNil => fn(a, b, c, d) }
    }

  implicit def fnHLister5[A, B, C, D, E, Res]: Aux[(A, B, C, D, E) => Res, (A :: B :: C :: D :: E :: HNil) => Res] =
    new FnHLister[(A, B, C, D, E) => Res] {
      type Out = (A :: B :: C :: D :: E :: HNil) => Res
      def apply(fn: (A, B, C, D, E) => Res) = { case a :: b :: c :: d :: e :: HNil => fn(a, b, c, d, e) }
    }

  implicit def fnHLister6[A, B, C, D, E, F, Res]: Aux[(A, B, C, D, E, F) => Res, (A :: B :: C :: D :: E :: F :: HNil) => Res] =
    new FnHLister[(A, B, C, D, E, F) => Res] {
      type Out = (A :: B :: C :: D :: E :: F :: HNil) => Res
      def apply(fn: (A, B, C, D, E, F) => Res) = { case a :: b :: c :: d :: e :: f :: HNil => fn(a, b, c, d, e, f) }
    }

  implicit def fnHLister7[A, B, C, D, E, F, G, Res]: Aux[(A, B, C, D, E, F, G) => Res, (A :: B :: C :: D :: E :: F :: G :: HNil) => Res] =
    new FnHLister[(A, B, C, D, E, F, G) => Res] {
      type Out = (A :: B :: C :: D :: E :: F :: G :: HNil) => Res
      def apply(fn: (A, B, C, D, E, F, G) => Res) = { case a :: b :: c :: d :: e :: f :: g :: HNil => fn(a, b, c, d, e, f, g) }
    }

  implicit def fnHLister8[A, B, C, D, E, F, G, H, Res]: Aux[(A, B, C, D, E, F, G, H) => Res, (A :: B :: C :: D :: E :: F :: G :: H :: HNil) => Res] =
    new FnHLister[(A, B, C, D, E, F, G, H) => Res] {
      type Out = (A :: B :: C :: D :: E :: F :: G :: H :: HNil) => Res
      def apply(fn: (A, B, C, D, E, F, G, H) => Res) = { case a :: b :: c :: d :: e :: f :: g :: h :: HNil => fn(a, b, c, d, e, f, g, h) }
    }

  implicit def fnHLister9[A, B, C, D, E, F, G, H, I, Res]: Aux[(A, B, C, D, E, F, G, H, I) => Res, (A :: B :: C :: D :: E :: F :: G :: H :: I :: HNil) => Res] =
    new FnHLister[(A, B, C, D, E, F, G, H, I) => Res] {
      type Out = (A :: B :: C :: D :: E :: F :: G :: H :: I :: HNil) => Res
      def apply(fn: (A, B, C, D, E, F, G, H, I) => Res) = { case a :: b :: c :: d :: e :: f :: g :: h :: i :: HNil => fn(a, b, c, d, e, f, g, h, i) }
    }

  implicit def fnHLister10[A, B, C, D, E, F, G, H, I, J, Res]: Aux[(A, B, C, D, E, F, G, H, I, J) => Res, (A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: HNil) => Res] =
    new FnHLister[(A, B, C, D, E, F, G, H, I, J) => Res] {
      type Out = (A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: HNil) => Res
      def apply(fn: (A, B, C, D, E, F, G, H, I, J) => Res) = { case a :: b :: c :: d :: e :: f :: g :: h :: i :: j :: HNil => fn(a, b, c, d, e, f, g, h, i, j) }
    }

  implicit def fnHLister11[A, B, C, D, E, F, G, H, I, J, K, Res]: Aux[(A, B, C, D, E, F, G, H, I, J, K) => Res, (A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: HNil) => Res] =
    new FnHLister[(A, B, C, D, E, F, G, H, I, J, K) => Res] {
      type Out = (A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: HNil) => Res
      def apply(fn: (A, B, C, D, E, F, G, H, I, J, K) => Res) = { case a :: b :: c :: d :: e :: f :: g :: h :: i :: j :: k :: HNil => fn(a, b, c, d, e, f, g, h, i, j, k) }
    }

  implicit def fnHLister12[A, B, C, D, E, F, G, H, I, J, K, L, Res]: Aux[(A, B, C, D, E, F, G, H, I, J, K, L) => Res, (A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: HNil) => Res] =
    new FnHLister[(A, B, C, D, E, F, G, H, I, J, K, L) => Res] {
      type Out = (A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: HNil) => Res
      def apply(fn: (A, B, C, D, E, F, G, H, I, J, K, L) => Res) = { case a :: b :: c :: d :: e :: f :: g :: h :: i :: j :: k :: l :: HNil => fn(a, b, c, d, e, f, g, h, i, j, k, l) }
    }

  implicit def fnHLister13[A, B, C, D, E, F, G, H, I, J, K, L, M, Res]: Aux[(A, B, C, D, E, F, G, H, I, J, K, L, M) => Res, (A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: HNil) => Res] =
    new FnHLister[(A, B, C, D, E, F, G, H, I, J, K, L, M) => Res] {
      type Out = (A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: HNil) => Res
      def apply(fn: (A, B, C, D, E, F, G, H, I, J, K, L, M) => Res) = { case a :: b :: c :: d :: e :: f :: g :: h :: i :: j :: k :: l :: m :: HNil => fn(a, b, c, d, e, f, g, h, i, j, k, l, m) }
    }

  implicit def fnHLister14[A, B, C, D, E, F, G, H, I, J, K, L, M, N, Res]: Aux[(A, B, C, D, E, F, G, H, I, J, K, L, M, N) => Res, (A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: HNil) => Res] =
    new FnHLister[(A, B, C, D, E, F, G, H, I, J, K, L, M, N) => Res] {
      type Out = (A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: HNil) => Res
      def apply(fn: (A, B, C, D, E, F, G, H, I, J, K, L, M, N) => Res) = { case a :: b :: c :: d :: e :: f :: g :: h :: i :: j :: k :: l :: m :: n :: HNil => fn(a, b, c, d, e, f, g, h, i, j, k, l, m, n) }
    }

  implicit def fnHLister15[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, Res]: Aux[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O) => Res, (A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: HNil) => Res] =
    new FnHLister[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O) => Res] {
      type Out = (A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: HNil) => Res
      def apply(fn: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O) => Res) = { case a :: b :: c :: d :: e :: f :: g :: h :: i :: j :: k :: l :: m :: n :: o :: HNil => fn(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o) }
    }

  implicit def fnHLister16[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Res]: Aux[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P) => Res, (A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: HNil) => Res] =
    new FnHLister[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P) => Res] {
      type Out = (A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: HNil) => Res
      def apply(fn: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P) => Res) = { case a :: b :: c :: d :: e :: f :: g :: h :: i :: j :: k :: l :: m :: n :: o :: p :: HNil => fn(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p) }
    }

  implicit def fnHLister17[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, Res]: Aux[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q) => Res, (A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: HNil) => Res] =
    new FnHLister[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q) => Res] {
      type Out = (A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: HNil) => Res
      def apply(fn: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q) => Res) = { case a :: b :: c :: d :: e :: f :: g :: h :: i :: j :: k :: l :: m :: n :: o :: p :: q :: HNil => fn(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q) }
    }

  implicit def fnHLister18[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, Res]: Aux[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R) => Res, (A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: HNil) => Res] =
    new FnHLister[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R) => Res] {
      type Out = (A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: HNil) => Res
      def apply(fn: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R) => Res) = { case a :: b :: c :: d :: e :: f :: g :: h :: i :: j :: k :: l :: m :: n :: o :: p :: q :: r :: HNil => fn(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r) }
    }

  implicit def fnHLister19[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, Res]: Aux[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S) => Res, (A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: S :: HNil) => Res] =
    new FnHLister[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S) => Res] {
      type Out = (A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: S :: HNil) => Res
      def apply(fn: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S) => Res) = { case a :: b :: c :: d :: e :: f :: g :: h :: i :: j :: k :: l :: m :: n :: o :: p :: q :: r :: s :: HNil => fn(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s) }
    }

  implicit def fnHLister20[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, Res]: Aux[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T) => Res, (A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: S :: T :: HNil) => Res] =
    new FnHLister[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T) => Res] {
      type Out = (A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: S :: T :: HNil) => Res
      def apply(fn: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T) => Res) = { case a :: b :: c :: d :: e :: f :: g :: h :: i :: j :: k :: l :: m :: n :: o :: p :: q :: r :: s :: t :: HNil => fn(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t) }
    }

  implicit def fnHLister21[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, Res]: Aux[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U) => Res, (A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: S :: T :: U :: HNil) => Res] =
    new FnHLister[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U) => Res] {
      type Out = (A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: S :: T :: U :: HNil) => Res
      def apply(fn: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U) => Res) = { case a :: b :: c :: d :: e :: f :: g :: h :: i :: j :: k :: l :: m :: n :: o :: p :: q :: r :: s :: t :: u :: HNil => fn(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u) }
    }

  implicit def fnHLister22[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, Res]: Aux[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V) => Res, (A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: S :: T :: U :: V :: HNil) => Res] =
    new FnHLister[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V) => Res] {
      type Out = (A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: S :: T :: U :: V :: HNil) => Res
      def apply(fn: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V) => Res) = { case a :: b :: c :: d :: e :: f :: g :: h :: i :: j :: k :: l :: m :: n :: o :: p :: q :: r :: s :: t :: u :: v :: HNil => fn(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v) }
    }
}


