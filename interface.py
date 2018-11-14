# coding:utf-8
# class IFoo:
#     def fun1(self):
#         pass
#         raise Exception("错误提示")
#
#
# class Bar(IFoo):
#     def fun1(self):
#         # 方法名必须和父类中的方法名相同，不然没办法正常执行，会抛出异常
#         print("子类中如果想要调用父类中的方法，子类中必须要有父类中的方法名")
#
#     def fun2(self):
#         print("test")
#
#
# obj = Bar()
# obj.fun1()

import abc


class Foo(metaclass=abc.ABCMeta):
    def fun1(self):
        print("fun1")

    def fun2(self):
        print("fun2")

    @classmethod
    def fun3(self):
        print(self)
        pass


class Bar(Foo):
    def fun4(self):
        print("子类必须有父类的抽象方法名，不然会抛出异常")


obj = Bar()
obj.fun1()
obj.fun2()
obj.fun3()
Foo.fun3()