import { z } from "@gatwech/utils/zod";

const OrderPlacedSchema = z.object({
  orderId: z.string(),
  userId: z.uuid(),
  items: z.array(z.object({
    productId: z.string(),
    quantity: z.number().positive(),
    price: z.number().positive(),
  })),
  total: z.number().positive(),
  timestamp: z.number(),
});

type OrderPlaced = z.infer<typeof OrderPlacedSchema>;

// Test 1: Direct type inference
function test1(payload: OrderPlaced) {
  // This should work - TypeScript should infer item type
  payload.items.forEach(item => {
    console.log(item.productId); // Should this work?
  });
}

// Test 2: Using const type
function test2() {
  const payload: OrderPlaced = {
    orderId: "123",
    userId: "550e8400-e29b-41d4-a716-446655440000",
    items: [
      { productId: "p1", quantity: 1, price: 10.0 }
    ],
    total: 10.0,
    timestamp: Date.now()
  };
  
  payload.items.forEach(item => {
    console.log(item.productId);
  });
}
