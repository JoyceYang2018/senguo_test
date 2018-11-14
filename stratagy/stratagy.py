import inspect
import stratagy_func
func_list = [func for name, func in inspect.getmembers(stratagy_func, inspect.isfunction)]
class_list = [func for name, func in inspect.getmembers(stratagy_func, inspect.isclass)]
class_ = class_list[0]
func_in_class_list = [func for name, func in inspect.getmembers(class_, inspect.isfunction)]
print(func_in_class_list)