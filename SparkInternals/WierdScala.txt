[https://docs.scala-lang.org/tutorials/FAQ/finding-symbols.html]
[Find for: Identifiers ]
_____________________________________________________________________________________________________________________________________________________

[vararg expansion]

_*



: _* is a special instance of type ascription which tells the compiler to treat a single argument of a sequence type as a variable argument sequence,
i.e. varargs.

[https://stackoverflow.com/questions/2087250/what-is-the-purpose-of-type-ascriptions-in-scala]


def multiply(x:Int, y:Int) = {
    x*y;
}

val operands = (2, 4)
multiply _ tupled operands




def echo(args: String*) = 
    for (arg <- args) println(arg)

val arr = Array("What's", "up", "doc?")
echo(arr: _*)


_____________________________________________________________________________________________________________________________________________________

Methods ending in colon (:) bind to the right instead of the left.

1 +: List(2, 3) :+ 4
The first method (+:) binds to the right, and is found on List. The second method (:+) is just a normal method, and binds to the left – again, on List.

List(1, 2) ++ List(3, 4)
List(1, 2).++(List(3, 4))
List(2, 3).::(1)
1 +: List(2, 3) :+ 4


_____________________________________________________________________________________________________________________________________________________

[https://ananthakumaran.in/2010/03/29/scala-underscore-magic.html]

[Scala (_) underscore]



_____________________________________________________________________________________________________________________________________________________



