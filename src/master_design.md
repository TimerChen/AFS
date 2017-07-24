# Master
## 后台任务
### 实现

* 思路1: 使用extrie的iterate操作，该操作可以遍历extrie，也就是MetadataContainer。并对其中的每一个元素进行一定的操作。
  - 实现：ok
  - 问题：某些操作，比如说re-replication，在发现问题的时候，需要直接调用MetadataContainer以外的内容来完成操作，而使用iterate则无法简单的完成这样的行为。
  - 或许可行的方法
    1. lambda表达式+捕获列表。
    2. 函数对象实现类似于lambda表达式。
  - 目前采用的方法为2。原因在于需要进行的后台工作种类较多，所以分成不同的小函数（对象）写起来更加得清晰。
* 思路2: extrie的iterator
  - 实现：为了支持并行，需要在iterator中存储锁。并且对于iterator可以进行操作必然有一些限制。
  - 问题：写起来用起来太难受了。
