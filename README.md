# Understanding polars for people who suck in hardware science ( like me !)

Hello!

So for understanding, polars is a library created by Richie Vink, who wrote [I wrote one of the fastest dataframe librairies](https://www.ritchievink.com/blog/2021/02/28/i-wrote-one-of-the-fastest-dataframe-libraries/), which resembles a lot like pandas, created by Wes Mkinney, after he released his article [10 things I hate about pandas](https://wesmckinney.com/blog/apache-arrow-pandas-internals/).
To really understand the differences between those librairies, I needed to uderstand how it worked on a hardware.

## Part 1: The structure of polars

![polars](/OIP.jpg)



Polars uses __parallelization__ in a __multi-threading__ way for its operations, which means that some functions written in Polars will be processed by different cores of your CPU at the same time.
For instance, if you perform an operation on column1 and another on column2, it's highly probable that those functions are processed by the CPU simultaneously, thus __reducing the latency of those operations.__

Also, Polars uses __SIMD (Single Instruction Multiple Data)__ structure to process entire columns. SIMD allows a single CPU instruction to process multiple data in the same time.
And, actually, this is the purpose of Polars! Its aim is not to apply functions row by row but to apply them to entire columns simultaneously.

### And for memory:
The memory backend of Polars is __Arrow__, not PyArrow like Pandas with an Arrow backend, but rather Arrow for Rust. Since Polars uses a columnar type of storage, where data is stored column-wise instead of row-wise, it is highly efficient for analytical queries that typically operate on entire columns. This approach minimizes memory access and enhances cache efficiency.

#### _What is cache?_
__A cache__ is a small, fast memory storage area that temporarily holds frequently accessed data to speed up subsequent data retrievals. __Its goal is to reduce the time__ it takes to access data from the main memory or storage, thus making data loading quicker.

When the __CPU__ is instructed to fetch data, __it initially checks its caches__, which typically include three levels: L1, L2, and L3. If the requested data __is found__ in any of these caches (known as a __cache hit__), the CPU retrieves it quickly. However, if the data __is not present in the cache__ (resulting in a __cache miss__), the CPU must retrieve it from the main memory storage, which inevitably takes more time.

This explains why reloading a dataset in a notebook or any other software application may be faster the second time. If the data remains stored in the cache from a previous load, subsequent accesses to that data can be served from the cache, leading to quicker retrieval times and improved performance.
[More informations here](https://www.spiceworks.com/tech/tech-101/articles/what-is-cache/).


#### So, because Polars uses columnar storage, it means that it stores entire columns together. whether as in row storage, rows are stored together.
If I Could write an example:

__for row-storage:__
```
Row 1: [A1, B1, C1, D1]
Row 2: [A2, B2, C2, D2]
Row 3: [A3, B3, C3, D3]
```
if you use __row storage__, your __memory layout__ ( of usually 64 bits ) looks like this:
```
[A1, B1, C1, D1, A2, B2, C2, D2, A3, B3, C3, D3, ...]
```
So, if you need to CPU to, let's say, do an operation on the first column, it will need to skip datas, while still loading them because for your CPU to go through your list, it needs to load your datas that they are "seeing".
Therefore, it takes space in the cache to load all of your data in you memory list. And some of the datas that is not used can be evicted from the cache to quickly, leading to more cache misses when you'll need them in the future.
__Row storage maximizes cache miss.__

__for column storage__, you have then: 
```
Column A: [A1, A2, A3, ...]
Column B: [B1, B2, B3, ...]
Column C: [C1, C2, C3, ...]
Column D: [D1, D2, D3, ...]
```
which means that your storage list will look like this:

```
[A1, A2, A3, ..., B1, B2, B3, ..., C1, C2, C3, ..., D1, D2, D3, ...]
```
So it's a lot easier for your CPU to go fetching from that! __And it maxmizes cache hits.__

__That's one of the reason Polars is so fast!__

But, you might ask, since Pandas has a new __memory-backend__ in __Arrow__, why is Polars faster? Since they are using the same memory_backend with a column storage for both.

#### Well it is because of the backend in RUST.
As you may know, the backend of pandas is written in C. For some background:

In your computer, there is a special memory area called the __stack__ where, sometimes, there are __pointers__ and other data types. (Pointers can exist outside of the stack, but for now, let's focus on the stack.) Basically, __a pointer stores memory addresses__ where other data is stored.

There is also the __heap__, another memory region used for dynamic memory allocation. When memory is allocated on the heap using functions like malloc() or new, a pointer to the allocated memory block is returned. These pointers can then be stored on the stack or elsewhere for later use.

In languages like C, programmers are responsible for managing memory explicitly. While the instructions for deallocating memory are well-defined, improper memory management can lead to issues such as memory leaks and degraded system performance, including slower execution and potential memory exhaustion. Additionally, in C, when data exceeds the allocated memory space, it overwrites adjacent memory, leading to undefined behavior, which means that the behavior of the program is not specified by the C standard and can result in crashes, corrupted data, etc.

Furthermore, with pointers, if they are not used properly (for instance, if you use a pointer that points to data that was deleted), it can lead to memory corruption and other issues that your computer won't like.

In Rust, lifetimes are explicitly annotated in function signatures and data structures, providing clarity and safety in memory management and avoiding deallocated memory like in C. Rust also has automatic memory management, so you don't need to manage it manually like in C, making it more effective in memory management.

Mores infos here : { [Memory Address](https://www.w3schools.com/c/c_memory_address.php)

 [Pointers](https://www.w3schools.com/c/c_pointers.php) 
 
  [Really Nice Stack Overflow discussion about C memory management](https://stackoverflow.com/questions/24891/c-memory-management)
  
  [How Rust manages memory](https://medium.com/geekculture/understanding-memory-management-in-rust-a341cfce9807) }



### Ok, enough about the geek stuff, let's talk about the lazy API.

## Part 2: The APIs
Polars has an eager and a Lazy API.
The principle of the lazy API is that it doesn't apply the functions you write, but makes a query plan of your fonctions, rearranging them to be the most efficient as possible.
To repass in eager mode, you need to apply _.collect()_ function to your dataframe.
There is also the scan\__format_ function that directly "loads" your data in a lazyframe. But it doesn't really load them, it's just another fuction in your query plan.
To save your function, you need to use a function called __sink__\__format_, which basically does a dataframe.collect().write\__format_, but goes into streaming mode. The streaming mode is using chunks of your dataset, apply your query plan, store it, and moves on to the next chunk.

Thus, the Lazy Api becomes really handy for working with out of memory dataset.

__!! Disclaimer !! :__ The streaming mode is considered unstable for now. I think it will be corrected in the very near future though ;) .
You could read the doc on LazyFrame [here](https://docs.pola.rs/user-guide/concepts/lazy-vs-eager/).
You could see in example_lazy_1.ipynb what ressembles a quary plan. You can see that the query is not planning to execute your functions in the same order that you put them. because the lazy frame is using query optimization, it will reorganise your functions for your computer to process them more efficiently, privileging parrallel operations and removing irrelevant operations for instance.



## Part 3 : Admire that speed
![Flash](/flash.avif)
I put some example of the difference of execution speed using polars in example_2.ipynb. You can see for yourself the difference.

### Also, I would like to make a quick point on how polars fosters method chaining. 

![polars](/method.png)

In this example ( that is example_lazy_1.ipynb ), you can see how the written structure of polars is really slender and is pushing you to use method chaining.
Because, in pandas, you have 1000 ways to write the same things with different functions, you can easely find yourself lost.
In polars, you don't have that much variability in your syntaxe, which makes it better for maintenance and for understanding your code with your peers ( if you need to share it or to understand it years later).

## Ok I think I said everything I had to say.
Thank you!
















