# Clean Code Guidelines for TypeScript Developers  
_Based on labs42io/clean-code-typescript_

## 📌 Philosophy  
Clean Code is about writing **readable**, **maintainable**, and **refactorable** code.  
These are **guidelines**, not strict rules. First drafts can be messy — refactor later.

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

### ✔ Use default object values when possible  
```ts
function createMenu(options: MenuOptions = {}) {}
```

### ✔ No Boolean flags  
```ts
// Bad
getData(true);

// Good
getCachedData();
```

### ✔ Avoid side effects  
Don’t mutate parameters or external shared state.

### ✔ Avoid global pollution  
Never extend native prototypes like `Array.prototype`.

### ✔ Prefer functional patterns  
Use `map`, `reduce`, `filter` when appropriate.

### ✔ Encapsulate conditionals  
```ts
if (isValidUser(user)) {}
```

### ✔ Avoid negative conditionals  
```ts
if (isEnabled) {} // not: if (!disabled)
```

### ✔ Use polymorphism over conditionals  
Avoid large if/else or switch trees.

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
```ts
const settings: Readonly<Settings> = { ... };
```

### ✔ Use `as const` for constant objects  
```ts
const ROLES = ['ADMIN', 'USER'] as const;
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

### ✔ Use exceptions for exceptional cases  
Avoid using return codes or magic values.

### ✔ Provide meaningful error messages  
```ts
throw new Error("User not found: " + userId);
```

### ✔ Don’t swallow errors  
Always propagate or handle meaningfully.

---

## 🧪 7. Testing Best Practices

### ✔ Use descriptive test names  
```ts
it("returns the correct total for a valid cart", () => {});
```

### ✔ Test behavior, not implementation  
Avoid relying on internal details.

### ✔ Prefer small, isolated unit tests  
One behavior per test.

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
- Prettier
- ESLint
- Biome

### ✔ Consistency > personal preference  

---

## 📚 10. Tooling Recommendations

- Use TypeScript strictly:
  - strict: true
  - noImplicitAny
  - noUnusedLocals
  - noUnusedParameters
- Run format + lint in CI.
- Use type-safe APIs & libraries.
