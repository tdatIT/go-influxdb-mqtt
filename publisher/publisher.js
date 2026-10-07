import mqtt from "mqtt";

const BROKER = process.env.MQTT_URL ?? "mqtt://localhost:1883";
const INTERVAL_MS = Number(process.env.INTERVAL_MS ?? 2000);

// 3 simulated vending machines. vm-003 has a weak compressor -> temperature keeps rising
const machines = [
    { id: "vm-001", site: "hcm-q1", temp: 4.0, drift: 0 },
    { id: "vm-002", site: "hcm-q1", temp: 5.0, drift: 0 },
    { id: "vm-003", site: "hn-cg", temp: 4.5, drift: 0.02 },
].map((m) => ({ ...m, stock: 40, humidity: 60 }));

const products = [
    { name: "coke", price: 15000 },
    { name: "water", price: 10000 },
    { name: "coffee", price: 20000 },
];
const payments = ["cash", "qr", "card"];
const pick = (arr) => arr[Math.floor(Math.random() * arr.length)];
const jitter = (n) => (Math.random() - 0.5) * n;

const client = mqtt.connect(BROKER, { clientId: `vending-sim-${process.pid}` });

client.on("connect", () => {
    console.log(`connected ${BROKER}, publishing every ${INTERVAL_MS}ms`);
    setInterval(tick, INTERVAL_MS);
});
client.on("error", (err) => console.error("mqtt error:", err.message));

function tick() {
    const now = Date.now();
    for (const m of machines) {
        // 1) Periodic telemetry (regular time series)
        m.temp = +(m.temp + m.drift + jitter(0.3)).toFixed(2);
        if (m.temp > 12) m.temp = 4.5; // technician fixed the machine
        m.humidity = +(m.humidity + jitter(1)).toFixed(1);
        const telemetry = {
            ts: now,
            temperature: m.temp,
            humidity: m.humidity,
            door_open: Math.random() < 0.05,
            stock: m.stock,
        };
        publish(`vending/${m.site}/${m.id}/telemetry`, telemetry);

        // 2) Sale events (irregular time series)
        if (m.stock > 0 && Math.random() < 0.3) {
            const p = pick(products);
            m.stock -= 1;
            publish(`vending/${m.site}/${m.id}/sale`, {
                ts: now,
                order_id: `${m.id}-${now}`,
                product: p.name,
                payment: pick(payments),
                price: p.price,
            });
        }
        if (m.stock === 0) m.stock = 40; // restock
    }
}

function publish(topic, payload) {
    client.publish(topic, JSON.stringify(payload), { qos: 1 }, (err) => {
        if (err) console.error("publish failed", topic, err.message);
    });
    console.log(topic, JSON.stringify(payload));
}

process.on("SIGINT", () => client.end(false, () => process.exit(0)));