<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->
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
* Don't use insert w/ big data, use file upload instead

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
                    
            // add function from hivemall class
            initBy(HQ.fromStatement("CREATE TEMPORARY FUNCTION hivemall_version as 'hivemall.HivemallVersionUDF'"));
        }
    };
    
    @ClassRule
    public static TDSystemTestRunner tRunner = new TDSystemTestRunner(ci) {
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
    public SystemTestTeam team = new SystemTestTeam(hRunner); // set hRunner as default runner
    
    @Rule
    public ExpectedException predictor = ExpectedException.none(); 


    @Test
    public void test0() throws Exception {
        team.add(tRunner, hRunner); // test on HiveRunner -> TD -> HiveRunner (NOTE: state of DB is retained in each runner)
        team.set(HQ.fromStatement("SELECT name FROM color WHERE blue = 255 ORDER BY name"), "azure\tblue\tmagenta", true); // ordered test
        team.run(); // this call is required
    }

    @Test
    public void test1() throws Exception {
        // test on HiveRunner once only
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
    
    @Test
    public void test2() throws Exception {
        // You can also use runner's raw API directly
        for(RawHQ q: HQ.fromStatements("SELECT hivemall_version();SELECT hivemall_version();")) {
            System.out.println(hRunner.exec(q).get(0));
        }
        // raw API doesn't require `SystemTestTeam#run()`
    }
    
    @Test
    public void test3() throws Exception {
        // test on HiveRunner once only
        // auto matching by files which name is `test3` in `case/` and `answer/`
        team.set(HQ.autoMatchingByFileName("test3"), ci); // unordered test
        team.run(); // this call is required
    }
    
    @Test
    public void test4() throws Exception {
        // test on HiveRunner once only
        predictor.expect(Throwable.class); // you can use systemtest w/ other rules 
        team.set(HQ.fromStatement("invalid queryyy"), "never used"); // this query throws an exception
        team.run(); // this call is required
        // thrown exception will be caught by `ExpectedException` rule
    }
}
```

The above requires following files

* `systemtest/src/test/resources/hivemall/QuickExample/init/color.tsv` (`systemtest/src/test/resources/${path/to/package}/${className}/init/${fileName}`)

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

* `systemtest/src/test/resources/hivemall/QuickExample/case/test3` (`systemtest/src/test/resources/${path/to/package}/${className}/case/${fileName}`)

```sql
-- write your hive queries
-- comments like this and multiple queries in one row are allowed
SELECT blue FROM color WHERE name = 'lavender';SELECT green FROM color WHERE name LIKE 'orange%' 
SELECT name FROM color WHERE blue = 255
```

* `systemtest/src/test/resources/hivemall/QuickExample/answer/test3` (`systemtest/src/test/resources/${path/to/package}/${className}/answer/${fileName}`)

tsv format is required

```tsv
250
165    69
azure    blue    magenta
```
