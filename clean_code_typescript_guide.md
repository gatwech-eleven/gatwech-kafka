# Clean Code Guidelines for TypeScript Developers

**Welcome to the team!** We prioritize **maintainability**, **readability**, and **robustness**. These guidelines adapt "Clean Code" concepts for TypeScript — treat code as **communication for your teammates**, not just instructions for the computer.

This guide summarizes the core rules and examples you should follow. Keep examples short, prefer clarity, and use the project tooling (Prettier, ESLint) to enforce formatting.

## 📌 Philosophy  
Clean Code is about writing **readable**, **maintainable**, and **refactorable** code.  
These are **guidelines**, not strict rules. First drafts can be messy — refactor later.


---

## 📋 Table of Contents

1. [Concurrency (Async)](#-0-concurrency-async)
2. [Naming & Variables](#-1-naming--variables)
3. [Functions](#-2-functions)
4. [Objects & Data Structures](#-3-objects--data-structures)
5. [Classes](#-4-classes)
6. [SOLID Principles](#-5-solid-principles-ts-edition)
7. [Error Handling](#️-6-error-handling)
8. [Testing Best Practices](#-7-testing-best-practices)
9. [Comments](#-8-comments)
10. [Formatting](#-9-formatting)
11. [Tooling Recommendations](#-10-tooling-recommendations)

---

## ⚡ 0. Concurrency (Async)

**Reference**: [Clean Code TypeScript - Concurrency](https://github.com/labs42io/clean-code-typescript#concurrency)

We prefer **async/await** over promise chains or callbacks for readability and simpler error handling.

### ✔ Key Principles
- **Prefer async/await** — keeps flow linear and easier to reason about
- **Avoid callback hell** — wrap callback APIs in promises where needed
- **Don't ignore rejected promises** — always handle rejections and log/propagate errors

### 1) Prefer Async/Await

**❌ Bad (Promise chains)**:
```ts
import { request } from "request-promise";

function getOptions(id: number) {
  return request
    .get(`https://api.example.com/items/${id}`)
    .then((response) => {
      return request.get(response.link);
    })
    .then((linkResponse) => {
      console.log(linkResponse);
    })
    .catch((err) => {
      console.error(err);
    });
}
```

**✅ Good (Async/Await)**:
```ts
import { request } from "request-promise";

async function getOptions(id: number): Promise<void> {
  try {
    const response = await request.get(`https://api.example.com/items/${id}`);
    const linkResponse = await request.get(response.link);
    console.log(linkResponse);
  } catch (err) {
    console.error(err);
  }
}
```

### 2) Avoid Callbacks

**❌ Bad**:
```ts
import fs from "fs";

function readConfigFile(
  path: string,
  callback: (err: Error | null, data?: string) => void,
) {
  fs.readFile(path, "utf8", (err, data) => {
    if (err) {
      callback(err);
      return;
    }
    callback(null, data);
  });
}
```

**✅ Good**:
```ts
import fs from "fs/promises";

async function readConfigFile(path: string): Promise<string> {
  return await fs.readFile(path, "utf8");
}
```

### 3) Don't Ignore Rejected Promises

**❌ Bad**:
```ts
async function processData() {
  getData()
    .then(data => console.log(data));
  // Missing .catch() - rejection will be unhandled!
}
```

**✅ Good**:
```ts
async function processData() {
  try {
    const data = await getData();
    console.log(data);
  } catch (error) {
    logger.error("Failed to process data", error);
    throw error; // Re-throw if caller should handle
  }
}
```

---


## 🔤 1. Naming & Variables

### ✔ Use meaningful names  
```ts
const value;
const left;
const right;
```

### ✔ Use pronounceable names  
```ts
const customerId; // not: cust_id
```

### ✔ Use consistent vocabulary  
If you use “client” in one part of the system, don’t call it “customer” elsewhere.

### ✔ Make names searchable  
Avoid magic numbers or unexplained constants.

### ✔ Prefer explanatory variables  
```ts
for (const [id, user] of users) {}
```

### ✔ Avoid mental mapping  
No cryptic shorthands when clarity is possible.

### ✔ Use Enums for intent  
```ts
enum Status {
  SUCCESS,
  FAILED,
}
```

---

## 🧩 2. Functions

### ✔ Keep functions small  
Functions should **do one thing**.

### ✔ Use 2 or fewer parameters  
If more: use an object.
```ts
function createUser({ name, email, role }: CreateUserOptions) {}
```

### ✔ Set default objects with Object.assign or destructuring

Avoid manually setting defaults with `||` operators. Use modern JavaScript/TypeScript features.

**❌ Bad**:
```ts
type MenuConfig = { 
  title?: string;
  body?: string;
  buttonText?: string;
  cancellable?: boolean;
};

function createMenu(config: MenuConfig) {
  config.title = config.title || 'Foo';
  config.body = config.body || 'Bar';
  config.buttonText = config.buttonText || 'Baz';
  config.cancellable = config.cancellable !== undefined ? config.cancellable : true;
  // ...
}

createMenu({ body: 'Bar' });
```

**✅ Good (Object.assign)**:
```ts
type MenuConfig = { 
  title?: string;
  body?: string;
  buttonText?: string;
  cancellable?: boolean;
};

function createMenu(config: MenuConfig) {
  const menuConfig = Object.assign({
    title: 'Foo',
    body: 'Bar',
    buttonText: 'Baz',
    cancellable: true
  }, config);
  // ...
}

createMenu({ body: 'Bar' });
```

**✅ Better (Spread operator)**:
```ts
function createMenu(config: MenuConfig) {
  const menuConfig = {
    title: 'Foo',
    body: 'Bar',
    buttonText: 'Baz',
    cancellable: true,
    ...config, // Overrides defaults with provided values
  };
  // ...
}
```

**✅ Best (Destructuring with defaults)**:
```ts
function createMenu({ 
  title = 'Foo',
  body = 'Bar',
  buttonText = 'Baz',
  cancellable = true
}: MenuConfig) {
  // All values have defaults, use them directly
}

createMenu({ body: 'Bar' });
```

**Note**: The spread operator and `Object.assign()` are similar. The main difference is that spreading **defines new properties**, while `Object.assign()` **sets them**.

**TypeScript tip**: Use `--strictNullChecks` to prevent `undefined` or `null` from being passed explicitly, avoiding unexpected behavior.



### ✔ Don't use flags as function parameters

Flags tell your user that this function does more than one thing. Functions should do one thing. Split out your functions if they are following different code paths based on a boolean.

**❌ Bad**:
```ts
function createFile(name: string, temp: boolean) {
  if (temp) {
    fs.create(`./temp/${name}`);
  } else {
    fs.create(name);
  }
}
```

**✅ Good**:
```ts
function createTempFile(name: string) {
  createFile(`./temp/${name}`);
}

function createFile(name: string) {
  fs.create(name);
}
```

### ✔ Avoid side effects (part 1)

A function produces a side effect if it does anything other than take a value in and return another value or values. A side effect could be writing to a file, modifying some global variable, or accidentally wiring all your money to a stranger.

Now, you do need to have side effects in a program on occasion. Like the previous example, you might need to write to a file. What you want to do is to **centralize where you are doing this**. Don't have several functions and classes that write to a particular file. Have one service that does it. One and only one.

The main point is to avoid common pitfalls like sharing state between objects without any structure, using mutable data types that can be written to by anything, and not centralizing where your side effects occur.

**❌ Bad**:
```ts
// Global variable referenced by following function.
let name = 'Robert C. Martin';

function toBase64() {
  name = btoa(name);
}

toBase64();
// If we had another function that used this name, now it'd be a Base64 value

console.log(name); // expected 'Robert C. Martin' but got 'Um9iZXJ0IEMuIE1hcnRpbg=='
```

**✅ Good**:
```ts
const name = 'Robert C. Martin';

function toBase64(text: string): string {
  return btoa(text);
}

const encodedName = toBase64(name);
console.log(name); // 'Robert C. Martin'
```

### ✔ Avoid side effects (part 2)

In JavaScript, objects and arrays are mutable values, so it's important to handle them carefully when they're passed as parameters to a function. A function can change an object's properties or alter the contents of an array which could easily cause bugs elsewhere.

**Example scenario**: The user clicks "Purchase" which sends a cart array to the server. Due to bad network connection, the function retries. Meanwhile, the user accidentally clicks "Add to Cart" on an unwanted item. The purchase function will send the accidentally added item because the cart array was modified.

**Solution**: Always clone the cart, edit it, and return the clone. This ensures that functions using the old shopping cart aren't affected by changes.

**❌ Bad**:
```ts
function addItemToCart(cart: CartItem[], item: Item): void {
  cart.push({ item, date: Date.now() });
}
```

**✅ Good**:
```ts
function addItemToCart(cart: CartItem[], item: Item): CartItem[] {
  return [...cart, { item, date: Date.now() }];
}
```

**Note**: Cloning big objects can be expensive, but modern libraries make this fast and memory-efficient.

### ✔ Avoid global pollution

Never extend native prototypes like `Array.prototype`. Polluting globals is bad practice because you could clash with another library.

**❌ Bad**:
```ts
declare global {
  interface Array<T> {
    diff(other: T[]): Array<T>;
  }
}

if (!Array.prototype.diff) {
  Array.prototype.diff = function <T>(other: T[]): T[] {
    const hash = new Set(other);
    return this.filter(elem => !hash.has(elem));
  };
}
```

**✅ Good**:
```ts
class MyArray<T> extends Array<T> {
  diff(other: T[]): T[] {
    const hash = new Set(other);
    return this.filter(elem => !hash.has(elem));
  }
}
```

### ✔ Prefer functional patterns  
Use `map`, `reduce`, `filter` when appropriate.

**❌ Bad**:
```ts
const contributions = [
  { name: 'Uncle Bobby', linesOfCode: 500 },
  { name: 'Suzie Q', linesOfCode: 1500 },
  { name: 'Jimmy Gosling', linesOfCode: 150 },
  { name: 'Gracie Hopper', linesOfCode: 1000 }
];

let totalOutput = 0;

for (let i = 0; i < contributions.length; i++) {
  totalOutput += contributions[i].linesOfCode;
}
```

**✅ Good**:
```ts
const contributions = [
  { name: 'Uncle Bobby', linesOfCode: 500 },
  { name: 'Suzie Q', linesOfCode: 1500 },
  { name: 'Jimmy Gosling', linesOfCode: 150 },
  { name: 'Gracie Hopper', linesOfCode: 1000 }
];

const totalOutput = contributions
  .reduce((totalLines, output) => totalLines + output.linesOfCode, 0);
```

### ✔ Encapsulate conditionals  

Extract complex conditionals into well-named functions.

**❌ Bad**:
```ts
if (subscription.isTrial || account.balance > 0) {
  // ...
}
```

**✅ Good**:
```ts
function canActivateService(subscription: Subscription, account: Account) {
  return subscription.isTrial || account.balance > 0;
}

if (canActivateService(subscription, account)) {
  // ...
}
```

### ✔ Avoid negative conditionals  

**❌ Bad**:
```ts
function isEmailNotUsed(email: string): boolean {
  // ...
}

if (isEmailNotUsed(email)) {
  // ...
}
```

**✅ Good**:
```ts
function isEmailUsed(email: string): boolean {
  // ...
}

if (!isEmailUsed(email)) {
  // ...
}
```

**Why?** Positive conditionals are easier to read and reason about.

### ✔ Avoid conditionals - Use polymorphism instead

This seems like an impossible task. Upon first hearing this, most people say, "how am I supposed to do anything without an `if` statement?" The answer is that you can use **polymorphism** to achieve the same task in many cases.

**Why?** When you have classes and functions that have `if` statements, you are telling your user that your function does more than one thing. Remember: **a function should only do one thing**.

**❌ Bad**:
```ts
class Airplane {
  private type: string;
  // ...

  getCruisingAltitude() {
    switch (this.type) {
      case '777':
        return this.getMaxAltitude() - this.getPassengerCount();
      case 'Air Force One':
        return this.getMaxAltitude();
      case 'Cessna':
        return this.getMaxAltitude() - this.getFuelExpenditure();
      default:
        throw new Error('Unknown airplane type.');
    }
  }

  private getMaxAltitude(): number {
    // ...
  }
}
```

**✅ Good**:
```ts
abstract class Airplane {
  protected getMaxAltitude(): number {
    // shared logic with subclasses ...
  }

  // Each subclass implements its own cruising altitude logic
  abstract getCruisingAltitude(): number;
}

class Boeing777 extends Airplane {
  // ...
  getCruisingAltitude() {
    return this.getMaxAltitude() - this.getPassengerCount();
  }
}

class AirForceOne extends Airplane {
  // ...
  getCruisingAltitude() {
    return this.getMaxAltitude();
  }
}

class Cessna extends Airplane {
  // ...
  getCruisingAltitude() {
    return this.getMaxAltitude() - this.getFuelExpenditure();
  }
}
```

**Benefits**:
- Each class does **one thing**
- Easy to add new airplane types without modifying existing code (Open/Closed Principle)
- No complex switch statements to maintain
- Type-safe - TypeScript ensures all subclasses implement required methods

**When to use conditionals vs polymorphism**:
- **Use conditionals**: Simple, one-time checks (validation, null checks)
- **Use polymorphism**: Type-based behavior that varies by class



### ✔ Avoid manual type checking  
Leverage TypeScript’s type system instead of `instanceof`, etc.

### ✔ Don’t prematurely optimize  
Write clean code first; optimize later.

### ✔ Delete dead code  
Version control remembers everything.

### ✔ Use generators/iterators for complex collections  
Useful for pipelines or lazy evaluation.

---

## 🧱 3. Objects & Data Structures

### ✔ Use getters/setters for encapsulation  
Useful for validation, lazy loading, computed values.

### ✔ Protect class invariants  
Use:
- private
- protected
- readonly

### ✔ Prefer immutability

TypeScript's type system allows you to mark individual properties on an interface/class as `readonly`. This allows you to work in a functional way (an unexpected mutation is bad).

**For interfaces and objects**:

**❌ Bad**:
```ts
interface Config {
  host: string;
  port: string;
  db: string;
}
```

**✅ Good**:
```ts
interface Config {
  readonly host: string;
  readonly port: string;
  readonly db: string;
}
```

**For arrays, use `ReadonlyArray<T>`**:

**❌ Bad**:
```ts
const array: number[] = [1, 3, 5];
array = []; // error
array.push(100); // array will be updated ⚠️
```

**✅ Good**:
```ts
const array: ReadonlyArray<number> = [1, 3, 5];
array = []; // error
array.push(100); // error ✅
// Can use concat() and slice() which don't mutate
const newArray = array.concat([7, 9]); // ✅
```

**For function parameters (TypeScript 3.4+)**:

```ts
function processItems(items: readonly string[]) {
  items.push('new'); // error ✅
  return items.map(item => item.toUpperCase()); // ✅
}
```

### ✔ Use `as const` for literal values

For more advanced scenarios, use `const assertions` to create deeply immutable objects.

**❌ Bad**:
```ts
const config = {
  hello: 'world'
};
config.hello = 'changed'; // value is changed ⚠️

const array = [1, 3, 5];
array[0] = 10; // value is changed ⚠️

// writable objects are returned
function readonlyData(value: number) {
  return { value };
}

const result = readonlyData(100);
result.value = 200; // value is changed ⚠️
```

**✅ Good**:
```ts
// read-only object
const config = {
  hello: 'world'
} as const;
config.hello = 'changed'; // error ✅

// read-only array
const array = [1, 3, 5] as const;
array[0] = 10; // error ✅

// You can return read-only objects
function readonlyData(value: number) {
  return { value } as const;
}

const result = readonlyData(100);
result.value = 200; // error ✅
```

**Use `Readonly<T>` for complex types**:

```ts
interface Settings {
  theme: string;
  language: string;
  notifications: boolean;
}

const settings: Readonly<Settings> = {
  theme: 'dark',
  language: 'en',
  notifications: true
};

settings.theme = 'light'; // error ✅
```



### ✔ Use the right abstraction  
- interface → good for extending/implementing  
- type → good for unions/intersections

---

## 🧱 4. Classes

### ✔ Single Responsibility Principle  
Each class should handle **one concept**.

### ✔ High cohesion, low coupling  
Class members should relate closely; dependencies minimal.

### ✔ Prefer composition over inheritance  
Avoid deep inheritance chains.

### ✔ Support method chaining  
```ts
builder.setName("John").setAge(25).build();
```

---

## 🧭 5. SOLID Principles (TS Edition)

### S — Single Responsibility  
Every module/class should have one reason to change.

### O — Open/Closed  
Open for extension, closed for modification.

### L — Liskov Substitution  
Subclasses should behave like their base class.

### I — Interface Segregation  
Prefer small interfaces over large “fat” ones.

### D — Dependency Inversion  
Depend on abstractions, not concrete implementations.

---

## ⚠️ 6. Error Handling

### ✔ Don't ignore caught errors

**Logging to console.log is usually not enough**. Use your application's logger or rethrow when appropriate.

**❌ Bad**:
```ts
try {
  functionThatMightThrow();
} catch (error) {
  console.log(error); // Not enough!
}
```

**✅ Good**:
```ts
import { logger } from "./logging";

try {
  functionThatMightThrow();
} catch (error) {
  logger.error("Failed to execute function", { error });
  // Re-throw if caller should handle
  throw error;
}
```

### ✔ Use exceptions for exceptional cases

Avoid using return codes or magic values.

**❌ Bad**:
```ts
function getUser(id: string): User | null {
  // Returns null on error - caller must check
  try {
    return database.findUser(id);
  } catch {
    return null;
  }
}
```

**✅ Good**:
```ts
function getUser(id: string): User {
  // Throws on error - caller can use try/catch
  const user = database.findUser(id);
  if (!user) {
    throw new NotFoundError(`User not found: ${id}`);
  }
  return user;
}
```

### ✔ Provide meaningful error messages

Include context that helps debugging.

**❌ Bad**:
```ts
throw new Error("Invalid input");
```

**✅ Good**:
```ts
throw new Error(`User not found: ${userId}`);
throw new BadRequestError(`Transfer amount must be positive, got: ${amount}`);
```

### ✔ Don't swallow errors

Always propagate or handle meaningfully.

**❌ Bad**:
```ts
try {
  await criticalOperation();
} catch (error) {
  // Silently ignored - very dangerous!
}
```

**✅ Good**:
```ts
try {
  await criticalOperation();
} catch (error) {
  logger.error("Critical operation failed", { error, context });
  // Either handle it or re-throw
  throw new OperationFailedError("Critical operation failed", { cause: error });
}
```

---

## 🧪 7. Testing Best Practices

### ✔ Unit tests required for business logic

Write tests for all critical business logic and edge cases.

### ✔ The Three Laws of TDD

**Follow these laws strictly when practicing TDD**:

1. **You are not allowed to write any production code unless it is to make a failing unit test pass.**

2. **You are not allowed to write any more of a unit test than is sufficient to fail; compilation failures are failures.**

3. **You are not allowed to write any more production code than is sufficient to pass the one failing unit test.**

**The Red-Green-Refactor Cycle**:
```
🔴 RED    → Write a failing test (just enough to fail)
🟢 GREEN  → Write minimum code to pass the test
🔵 REFACTOR → Clean up code while keeping tests green
```

**Example of following the Three Laws**:

```typescript
// Law 1 & 2: Write ONLY enough test to fail
describe('WalletService', () => {
  it('should create wallet with zero balance', async () => {
    const wallet = await walletService.createWallet(
      'user123',
      SupportedCurrency.USD,
      WalletOwnerType.CUSTOMER
    );
    // Test fails: createWallet doesn't exist yet
  });
});

// Law 3: Write ONLY enough code to pass this test
class WalletService {
  async createWallet(ownerId: string, currency: SupportedCurrency, ownerType: WalletOwnerType) {
    return this.prisma.wallet.create({
      data: {
        userId: ownerId,
        currency,
        ownerType,
        balance: "0",
        lockedFunds: "0",
      },
    });
  }
}

// Now test passes ✅
// Continue with next test...
```

### ✔ Prefer TDD when practical

Write the failing test first, then implement the feature.

### ✔ Use descriptive test names

**❌ Bad**:
```ts
it("works", () => {});
it("test1", () => {});
```

**✅ Good**:
```ts
it("returns the correct total for a valid cart", () => {});
it("throws NotFoundError when wallet does not exist", () => {});
it("prevents double-spend with concurrent transactions", () => {});
```

### ✔ One concept per test

Keep tests focused on a single behavior.

**❌ Bad (multiple assertions in one test)**:
```ts
import { assert } from "chai";

describe("AwesomeDate", () => {
  it("handles date boundaries", () => {
    let date: AwesomeDate;

    date = new AwesomeDate("1/1/2015");
    assert.equal("1/31/2015", date.addDays(30));

    date = new AwesomeDate("2/1/2016");
    assert.equal("2/29/2016", date.addDays(28));

    date = new AwesomeDate("2/1/2015");
    assert.equal("3/1/2015", date.addDays(28));
  });
});
```

**✅ Good (single concept per test)**:
```ts
import { assert } from "chai";

describe("AwesomeDate", () => {
  it("handles 30-day months", () => {
    const date = new AwesomeDate("1/1/2015");
    assert.equal("1/31/2015", date.addDays(30));
  });

  it("handles leap year", () => {
    const date = new AwesomeDate("2/1/2016");
    assert.equal("2/29/2016", date.addDays(28));
  });

  it("handles non-leap year", () => {
    const date = new AwesomeDate("2/1/2015");
    assert.equal("3/1/2015", date.addDays(28));
  });
});
```

### ✔ Test behavior, not implementation

Avoid relying on internal details - test the public API.

**❌ Bad**:
```ts
it("calls internal method twice", () => {
  const spy = jest.spyOn(service, "_internalMethod");
  service.publicMethod();
  expect(spy).toHaveBeenCalledTimes(2);
});
```

**✅ Good**:
```ts
it("creates transaction with correct amount", () => {
  const result = service.createTransaction({ amount: 100 });
  expect(result.amount).toBe(100);
  expect(result.status).toBe("COMPLETED");
});
```

### ✔ Prefer small, isolated unit tests

One behavior per test, minimal setup.

**✅ Good**:
```ts
describe("WalletService.createWallet", () => {
  it("creates wallet with zero balance", async () => {
    const wallet = await walletService.createWallet(
      "user123",
      SupportedCurrency.USD,
      WalletOwnerType.CUSTOMER
    );
    
    expect(wallet.balance.toString()).toBe("0");
    expect(wallet.lockedFunds.toString()).toBe("0");
  });
});
```

---

## 💬 8. Comments

### ✔ Explain “why,” not “what”  
The code should explain the “what.”

### ✔ Avoid noisy or obvious comments  
```ts
// Bad:
i += 1; // increment i
```

### ✔ Document complex business logic  
Especially domain-heavy logic.

---

## 🧹 9. Formatting

### ✔ Stick to one formatting standard

Use tools like:
- **Prettier** - automatic code formatting
- **ESLint** - TypeScript lint rules
- **Biome** - alternative all-in-one tool

### ✔ Consistency > personal preference

The team's formatting standard matters more than individual preferences.

### ✔ Do not disable ESLint rules without justification

**❌ Bad**:
```ts
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const data: any = response;
```

**✅ Good**:
```ts
// Define proper type instead
interface ApiResponse {
  data: unknown;
  status: number;
}
const response: ApiResponse = await api.fetch();
```

**✅ Acceptable (when `any` is necessary)**:
```ts
// When dealing with dynamic Prisma transaction clients or third-party libraries
// where proper typing is not available, document why `any` is needed
// eslint-disable-next-line @typescript-eslint/no-explicit-any
async function executeInTransaction(tx: any) {
  // Prisma's transaction client type is complex and varies by schema
  // Using `any` here to avoid type gymnastics while maintaining functionality
  return tx.wallet.findUnique({ where: { id: '123' } });
}

// Better: Use Prisma's built-in transaction type when possible
import type { Prisma } from "@/generated/prisma/client";

async function executeInTransaction(tx: Prisma.TransactionClient) {
  return tx.wallet.findUnique({ where: { id: '123' } });
}
```

**When `any` is acceptable**:
1. **Third-party library limitations** - When a library doesn't provide proper types
2. **Prisma dynamic clients** - Use `Prisma.TransactionClient` when possible, but `any` is acceptable if needed
3. **Complex generic scenarios** - When TypeScript's type inference fails and proper typing would be overly complex
4. **Migration from JavaScript** - Temporary `any` during gradual TypeScript migration

**Always include**:
- ESLint disable comment with reason
- TODO comment if it should be fixed later
- Documentation explaining why `any` is necessary

```ts
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const legacyData: any = oldLibrary.getData();
// TODO: Replace with proper type when upgrading to v2.0 which has TypeScript support
```


### ✔ Run format + lint in CI

Ensure all code is formatted and linted before merging:
```json
{
  "scripts": {
    "lint": "eslint .",
    "lint:fix": "eslint . --fix",
    "format": "prettier --write .",
    "typecheck": "tsc --noEmit"
  }
}
```

---

## 📚 10. Tooling Recommendations

- Use TypeScript strictly:
  - strict: true
  - noImplicitAny
  - noUnusedLocals
  - noUnusedParameters
- Run format + lint in CI.
- Use type-safe APIs & libraries.