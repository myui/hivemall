## Usage

### Initialization

Define `CommonInfo`, `Runner` and `Team` in each your test class.

### `CommonInfo`

* `SystemTestCommonInfo`

`CommonInfo` holds common information of test class, for example,
you can refer to auto-defined path to resources. This should be defined as `private static`.


### `Runner`

* `HiveSystemTestRunner`
* `TDSystemTestRunner`

`Runner` represents a test environment and its configuration. This must be defined with `@ClassRule`
as `public static` because of JUnit spec. You can add test class initializations by `#initBy(...)`
with class methods of `HQ`, which are abstract domain-specific hive queries, in instance initializer
of each `Runner`.


### `Team`

* `SystemTestTeam`

`Team` manages `Runner`s each test method. This must be defined with `@Rule` as `public` because of
JUnit spec. You can set `Runner`s via constructor argument as common in class and via `#add(...)`
as method-local and add test method initializations by `#initBy(...)` and test case by `#set(...)`
with class methods of `HQ`. Finally, don't forget call `#run()` to enable set `HQ`s.
As an alternative method, by `#set(HQ.autoMatchingByFileName(${filename}))` with queries predefined in
`auto-defined/path/init/${filename}`, `auto-defined/path/case/${filename}` and
`auto-defined/path/answer/${filename}`, you can do auto matching test.


## Notice

* DDL and insert statement should be called via class methods of `HQ` because of wrapping hive queries
and several runner-specific APIs, don't call them via string statement
* Also you can use low-level API via an instance of `Runner`, independent of `Team`
* You can use `IO.getFromResourcePath(...)` to get answer whose format is TSV


## Quick example

```java
package hivemall;
// here is several imports
public class HogeTest {
    private static SystemTestCommonInfo ci = new SystemTestCommonInfo(HogeTest.class);

    @ClassRule
    public static HiveSystemTestRunner hRunner = new HiveSystemTestRunner(ci) {
        {
            initBy(HQ.uploadByResourcePathAsNewTable("people", ci.initDir + "people.tsv",
                    new LinkedHashMap<String, String>() {
                        {
                            put("name", "string");
                            put("age", "int");
                            put("sex", "string");
                        }
                    }));
            initBy(HQ.fromResourcePath(ci.initDir + "init"));
        }
    };

    @Rule
    public SystemTestTeam team = new SystemTestTeam(hRunner);


    @Test
    public void insertAndSelectTest() throws Exception {
        String tableName = "people0";
        team.initBy(HQ.createTable(tableName, new LinkedHashMap<String, String>() {
            {
                put("name", "string");
                put("age", "int");
                put("sex", "string");
            }
        }));
        team.initBy(HQ.insert(tableName, Arrays.asList("name", "age", "sex"), Arrays.asList(
                new Object[]{"Noah", 46, "Male"}, new Object[]{"Isabella", 20, "Female"})));
        team.set(HQ.fromStatement("SELECT name FROM people WHERE age = 20"), "Jacob\tIsabella");
        team.set(HQ.fromStatement("SELECT COUNT(1) FROM " + tableName), "2");
        team.run();
    }
}
```

The above needs `systemtest/src/test/resources/hivemall/HogeTest/init/people.tsv` (`systemtest/src/test/resources/${path/to/package}/${classname}/init/people.tsv`)

```tsv
Jacob	20	Male
Mason	22	Male
Sophia	35	Female
Ethan	55	Male
Emma	15	Female
Noah	46	Male
Isabella	20	Female
```
