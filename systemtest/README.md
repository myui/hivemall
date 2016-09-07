## Usage

### Initialization

Define `CommonInfo`, `Runner` and `Team` in each your test class.

#### `CommonInfo`

* `SystemTestCommonInfo`

`CommonInfo` holds common information of test class, for example,
you can refer to auto-defined path to resources. This should be defined as `private static`.


#### `Runner`

* `HiveSystemTestRunner`
* `TDSystemTestRunner`

`Runner` represents a test environment and its configuration. This must be defined with `@ClassRule`
as `public static` because of JUnit spec. You can add test class initializations by `#initBy(...)`
with class methods of `HQ`, which are abstract domain-specific hive queries, in instance initializer
of each `Runner`.


#### `Team`

* `SystemTestTeam`

`Team` manages `Runner`s each test method. This must be defined with `@Rule` as `public` because of
JUnit spec. You can set `Runner`s via constructor argument as common in class and via `#add(...)`
as method-local and add test method initializations by `#initBy(...)` and test case by `#set(...)`
with class methods of `HQ`. Finally, don't forget call `#run()` to enable set `HQ`s.
As an alternative method, by `#set(HQ.autoMatchingByFileName(${filename}))` with queries predefined in
`auto-defined/path/init/${filename}`, `auto-defined/path/case/${filename}` and
`auto-defined/path/answer/${filename}`, you can do auto matching test.


### External properties

You can use external properties at `systemtest/src/test/resources/hivemall/*`, default is `hiverunner.properties`
for `HiveSystemTestRunner` and `td.properties` for `TDSystemTestRunner`. Also user-defined properties file can
be loaded via constructor of `Runner` by file name.


## Notice

* DDL and insert statement should be called via class methods of `HQ` because of wrapping hive queries
and several runner-specific APIs, don't call them via string statement
* Also you can use low-level API via an instance of `Runner`, independent of `Team`
* You can use `IO.getFromResourcePath(...)` to get answer whose format is TSV
* Table created in initialization of runner should be used as immutable, don't neither insert nor update
* TD client configs in properties file prior to $HOME/.td/td.conf

## Quick example

```java
package hivemall;
// here is several imports
public class QuickExample {
    private static SystemTestCommonInfo ci = new SystemTestCommonInfo(QuickExample.class);

    @ClassRule
    public static HiveSystemTestRunner hRunner = new HiveSystemTestRunner(ci) {
        {
            initBy(HQ.uploadByResourcePathAsNewTable("color", ci.initDir + "color.tsv",
                    new LinkedHashMap<String, String>() {
                        {
                            put("name", "string");
                            put("red", "int");
                            put("green", "int");
                            put("blue", "int");
                        }
                    })); // create table `color`, which is marked as immutable, for this test class
        }
    };

    @Rule
    public SystemTestTeam team = new SystemTestTeam(hRunner);


    @Test
    public void test0() throws Exception {
        team.set(HQ.fromStatement("SELECT name FROM color WHERE blue = 255 ORDER BY name"), "azure\tblue\tmagenta", true); // ordered test
        team.run(); // this call is required
    }

    @Test
    public void test1() throws Exception {
        String tableName = "users";
        team.initBy(HQ.createTable(tableName, new LinkedHashMap<String, String>() {
            {
                put("name", "string");
                put("age", "int");
                put("favorite_color", "string");
            }
        })); // create local table in this test method `users` for each set runner(only hRunner here)
        team.initBy(HQ.insert(tableName, Arrays.asList("name", "age", "favorite_color"), Arrays.asList(
                new Object[]{"Karen", 16, "orange"}, new Object[]{"Alice", 17, "pink"}))); // insert into `users`
        team.set(HQ.fromStatement("SELECT CONCAT('rgb(', red, ',', green, ',', blue, ')') FROM "
                + tableName + " u LEFT JOIN color c on u.favorite_color = c.name"), "rgb(255,165,0)\trgb(255,192,203)"); // unordered test
        team.run(); // this call is required
    }
}
```

The above needs `systemtest/src/test/resources/hivemall/HogeTest/init/color.tsv` (`systemtest/src/test/resources/${path/to/package}/${classname}/init/color.tsv`)

```tsv
blue	0	0	255
lavender	230	230	250
magenta	255	0	255
violet	238	130	238
purple	128	0	128
azure	240	255	255
lightseagreen	32	178	170
orange	255	165	0
orangered	255	69	0
red	255	0	0
pink	255	192	203
```
