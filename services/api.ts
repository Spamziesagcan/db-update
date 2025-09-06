import { Stats, Order } from '@/types';

async function fetcher(url: string, options?: RequestInit) {
    const response = await fetch(url, options);
    if (!response.ok) {
        const errorInfo = await response.json();
        throw new Error(errorInfo.detail || 'An API error occurred');
    }
    return response.json();
}

export async function getStats(baseUrl: string): Promise<Stats> {
    return fetcher(`${baseUrl}/stats`);
}

export async function getOrders(baseUrl: string, limit=20): Promise<Order[]> {
    return fetcher(`${baseUrl}/orders?limit=${limit}`);
}

export async function createOrder(baseUrl: string): Promise<{ success: boolean; order_id: number }> {
    const customers = ['Alice Johnson', 'Bob Smith', 'Carol Davis', 'David Wilson'];
    const products = ['Laptop', 'Phone', 'Tablet', 'Headphones', 'Monitor'];

    return fetcher(`${baseUrl}/orders`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            customer_name: customers[Math.floor(Math.random() * customers.length)],
            product_name: products[Math.floor(Math.random() * products.length)],
            status: 'pending',
        }),
    });
}

export async function updateRandomOrder(baseUrl: string, orderIds: number[]): Promise<{ success: boolean; order_id: number }> {
    const randomOrderId = orderIds[Math.floor(Math.random() * orderIds.length)];
    const statuses = ['shipped', 'delivered'];
    const randomStatus = statuses[Math.floor(Math.random() * statuses.length)];

    return fetcher(`${baseUrl}/orders/${randomOrderId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ status: randomStatus }),
    });
}

export async function deleteRandomOrder(baseUrl: string, orderIds: number[]): Promise<{ success: boolean; order_id: number }> {
    const randomOrderId = orderIds[Math.floor(Math.random() * orderIds.length)];
    return fetcher(`${baseUrl}/orders/${randomOrderId}`, {
        method: 'DELETE',
    });
}