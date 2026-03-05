import React, { useState, useEffect } from "react";

interface DashboardStats {
  total_customers: number;
  high_value_active: number;
  high_churn_risk: number;
  avg_ltv: number;
  avg_points: number;
  active_offers: number;
}

interface CustomerSummary {
  customer_id: string;
  first_name: string;
  last_name: string;
  loyalty_tier: string;
  points_balance: number;
  lifetime_value: number;
  segment: string;
  churn_risk_level: string;
}

interface CustomerProfile {
  customer_id: string;
  first_name: string;
  last_name: string;
  loyalty_tier: string;
  points_balance: number;
  lifetime_value: number;
  total_orders: number;
  next_tier_threshold: number;
  tier_progress_pct: number;
  segment: string;
  churn_risk_level: string;
  last_purchase_date: string;
  preferred_categories: string;
}

interface Offer {
  offer_id: string;
  offer_code: string;
  product_name: string;
  category: string;
  relevance_score: number;
  offer_type: string;
  discount_pct: number;
  expires_at: string;
}

interface ChatMessage {
  role: "user" | "assistant";
  content: string;
}

const TIER_COLORS: Record<string, string> = {
  Bronze: "#cd7f32",
  Silver: "#c0c0c0",
  Gold: "#ffd700",
  Platinum: "#e5e4e2",
};

const RISK_COLORS: Record<string, string> = {
  Low: "#22c55e",
  Medium: "#f59e0b",
  High: "#ef4444",
};

function App() {
  const [stats, setStats] = useState<DashboardStats | null>(null);
  const [customers, setCustomers] = useState<CustomerSummary[]>([]);
  const [searchQuery, setSearchQuery] = useState("");
  const [selectedCustomer, setSelectedCustomer] = useState<CustomerProfile | null>(null);
  const [offers, setOffers] = useState<Offer[]>([]);
  const [chatMessages, setChatMessages] = useState<ChatMessage[]>([]);
  const [chatInput, setChatInput] = useState("");
  const [chatLoading, setChatLoading] = useState(false);
  const [view, setView] = useState<"dashboard" | "customer">("dashboard");

  useEffect(() => {
    fetch("/api/dashboard/stats").then((r) => r.json()).then(setStats);
    fetch("/api/customers/search?limit=20").then((r) => r.json()).then((d) => setCustomers(d.customers));
  }, []);

  const searchCustomers = (q: string) => {
    setSearchQuery(q);
    fetch(`/api/customers/search?q=${encodeURIComponent(q)}&limit=20`)
      .then((r) => r.json())
      .then((d) => setCustomers(d.customers));
  };

  const selectCustomer = async (customerId: string) => {
    const [profileRes, offersRes] = await Promise.all([
      fetch(`/api/customer/${customerId}/profile`),
      fetch(`/api/customer/${customerId}/offers`),
    ]);
    const profile = await profileRes.json();
    const offersData = await offersRes.json();
    setSelectedCustomer(profile);
    setOffers(offersData.offers);
    setChatMessages([]);
    setView("customer");
  };

  const sendChat = async () => {
    if (!chatInput.trim() || !selectedCustomer) return;
    const userMsg = chatInput;
    setChatInput("");
    setChatMessages((prev) => [...prev, { role: "user", content: userMsg }]);
    setChatLoading(true);
    try {
      const res = await fetch(`/api/customer/${selectedCustomer.customer_id}/recommend`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message: userMsg, customer_id: selectedCustomer.customer_id }),
      });
      const data = await res.json();
      setChatMessages((prev) => [...prev, { role: "assistant", content: data.content || "Sorry, I couldn't process that request." }]);
    } catch {
      setChatMessages((prev) => [...prev, { role: "assistant", content: "Error connecting to Style Assistant." }]);
    }
    setChatLoading(false);
  };

  return (
    <div style={{ fontFamily: "'Segoe UI', system-ui, sans-serif", background: "#0f172a", color: "#e2e8f0", minHeight: "100vh" }}>
      {/* Header */}
      <header style={{ background: "#1e293b", borderBottom: "1px solid #334155", padding: "16px 32px", display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <div style={{ width: 36, height: 36, background: "linear-gradient(135deg, #3b82f6, #8b5cf6)", borderRadius: 8, display: "flex", alignItems: "center", justifyContent: "center", fontWeight: 700, fontSize: 18 }}>L</div>
          <h1 style={{ margin: 0, fontSize: 20, fontWeight: 600 }}>Loyalty Engine</h1>
        </div>
        <nav style={{ display: "flex", gap: 8 }}>
          <button onClick={() => setView("dashboard")} style={{ ...navBtn, background: view === "dashboard" ? "#3b82f6" : "transparent" }}>Dashboard</button>
          {selectedCustomer && (
            <button onClick={() => setView("customer")} style={{ ...navBtn, background: view === "customer" ? "#3b82f6" : "transparent" }}>
              {selectedCustomer.first_name}'s Profile
            </button>
          )}
        </nav>
      </header>

      <main style={{ maxWidth: 1200, margin: "0 auto", padding: 32 }}>
        {view === "dashboard" ? (
          <>
            {/* Stats Cards */}
            {stats && (
              <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 16, marginBottom: 32 }}>
                <StatCard label="Total Customers" value={stats.total_customers.toLocaleString()} />
                <StatCard label="High-Value Active" value={stats.high_value_active.toLocaleString()} color="#22c55e" />
                <StatCard label="High Churn Risk" value={stats.high_churn_risk.toLocaleString()} color="#ef4444" />
                <StatCard label="Avg LTV" value={`$${Number(stats.avg_ltv).toLocaleString()}`} />
                <StatCard label="Avg Points" value={stats.avg_points.toLocaleString()} />
                <StatCard label="Active Offers" value={stats.active_offers.toLocaleString()} color="#8b5cf6" />
              </div>
            )}

            {/* Customer Search */}
            <div style={{ background: "#1e293b", borderRadius: 12, padding: 24, border: "1px solid #334155" }}>
              <h2 style={{ margin: "0 0 16px", fontSize: 18 }}>Customers</h2>
              <input
                type="text"
                placeholder="Search by name or ID..."
                value={searchQuery}
                onChange={(e) => searchCustomers(e.target.value)}
                style={inputStyle}
              />
              <div style={{ marginTop: 16, overflowX: "auto" }}>
                <table style={{ width: "100%", borderCollapse: "collapse" }}>
                  <thead>
                    <tr style={{ borderBottom: "1px solid #334155" }}>
                      {["Customer", "Tier", "LTV", "Points", "Segment", "Churn Risk", ""].map((h) => (
                        <th key={h} style={{ textAlign: "left", padding: "8px 12px", fontSize: 12, color: "#94a3b8", fontWeight: 500 }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {customers.map((c) => (
                      <tr key={c.customer_id} style={{ borderBottom: "1px solid #1e293b", cursor: "pointer" }} onClick={() => selectCustomer(c.customer_id)}>
                        <td style={cellStyle}>
                          <div style={{ fontWeight: 500 }}>{c.first_name} {c.last_name}</div>
                          <div style={{ fontSize: 12, color: "#64748b" }}>{c.customer_id}</div>
                        </td>
                        <td style={cellStyle}><span style={{ color: TIER_COLORS[c.loyalty_tier] || "#e2e8f0", fontWeight: 600 }}>{c.loyalty_tier}</span></td>
                        <td style={cellStyle}>${Number(c.lifetime_value).toLocaleString()}</td>
                        <td style={cellStyle}>{c.points_balance?.toLocaleString()}</td>
                        <td style={cellStyle}><span style={{ background: "#334155", padding: "2px 8px", borderRadius: 12, fontSize: 12 }}>{c.segment}</span></td>
                        <td style={cellStyle}><span style={{ color: RISK_COLORS[c.churn_risk_level] || "#e2e8f0", fontWeight: 600 }}>{c.churn_risk_level}</span></td>
                        <td style={cellStyle}><button style={{ ...navBtn, background: "#3b82f6", fontSize: 12 }}>View</button></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </>
        ) : selectedCustomer ? (
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 24 }}>
            {/* Profile Card */}
            <div style={{ ...cardStyle, gridColumn: "1 / -1" }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "start" }}>
                <div>
                  <h2 style={{ margin: 0, fontSize: 24 }}>{selectedCustomer.first_name} {selectedCustomer.last_name}</h2>
                  <p style={{ margin: "4px 0 0", color: "#64748b" }}>{selectedCustomer.customer_id}</p>
                </div>
                <div style={{ textAlign: "right" }}>
                  <span style={{ color: TIER_COLORS[selectedCustomer.loyalty_tier], fontSize: 24, fontWeight: 700 }}>{selectedCustomer.loyalty_tier}</span>
                  <p style={{ margin: "4px 0 0", color: "#64748b" }}>{selectedCustomer.segment}</p>
                </div>
              </div>

              {/* Tier Progress */}
              <div style={{ margin: "20px 0" }}>
                <div style={{ display: "flex", justifyContent: "space-between", fontSize: 13, marginBottom: 6 }}>
                  <span>{selectedCustomer.points_balance.toLocaleString()} pts</span>
                  <span>{selectedCustomer.next_tier_threshold.toLocaleString()} pts to next tier</span>
                </div>
                <div style={{ background: "#334155", borderRadius: 8, height: 10, overflow: "hidden" }}>
                  <div style={{ background: `linear-gradient(90deg, #3b82f6, #8b5cf6)`, height: "100%", width: `${selectedCustomer.tier_progress_pct}%`, borderRadius: 8, transition: "width 0.5s" }} />
                </div>
              </div>

              <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 16 }}>
                <MiniStat label="Lifetime Value" value={`$${Number(selectedCustomer.lifetime_value).toLocaleString()}`} />
                <MiniStat label="Total Orders" value={selectedCustomer.total_orders?.toString() || "0"} />
                <MiniStat label="Last Purchase" value={selectedCustomer.last_purchase_date?.split("T")[0] || "N/A"} />
                <MiniStat label="Churn Risk" value={selectedCustomer.churn_risk_level} color={RISK_COLORS[selectedCustomer.churn_risk_level]} />
              </div>
            </div>

            {/* Offers */}
            <div style={cardStyle}>
              <h3 style={{ margin: "0 0 16px", fontSize: 16 }}>Personalized Offers</h3>
              {offers.length === 0 ? (
                <p style={{ color: "#64748b" }}>No active offers</p>
              ) : (
                <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
                  {offers.map((o) => (
                    <div key={o.offer_id} style={{ background: "#0f172a", borderRadius: 8, padding: 12, border: "1px solid #334155" }}>
                      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "start" }}>
                        <div>
                          <div style={{ fontWeight: 500, fontSize: 14 }}>{o.product_name}</div>
                          <div style={{ fontSize: 12, color: "#64748b", marginTop: 2 }}>{o.category} &middot; {o.offer_type}</div>
                        </div>
                        <div style={{ background: "#22c55e20", color: "#22c55e", padding: "2px 8px", borderRadius: 8, fontSize: 13, fontWeight: 600 }}>
                          {o.discount_pct}% OFF
                        </div>
                      </div>
                      <div style={{ display: "flex", justifyContent: "space-between", marginTop: 8, fontSize: 12, color: "#94a3b8" }}>
                        <span>Code: {o.offer_code}</span>
                        <span>Score: {o.relevance_score.toFixed(1)}</span>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>

            {/* Style Assistant Chat */}
            <div style={{ ...cardStyle, display: "flex", flexDirection: "column" }}>
              <h3 style={{ margin: "0 0 16px", fontSize: 16 }}>Style Assistant</h3>
              <div style={{ flex: 1, minHeight: 300, maxHeight: 400, overflowY: "auto", display: "flex", flexDirection: "column", gap: 8, marginBottom: 12 }}>
                {chatMessages.length === 0 && (
                  <p style={{ color: "#64748b", fontSize: 13 }}>Ask me for personalized product recommendations!</p>
                )}
                {chatMessages.map((msg, i) => (
                  <div key={i} style={{
                    alignSelf: msg.role === "user" ? "flex-end" : "flex-start",
                    background: msg.role === "user" ? "#3b82f6" : "#334155",
                    padding: "8px 14px", borderRadius: 12, maxWidth: "85%", fontSize: 14,
                    whiteSpace: "pre-wrap",
                  }}>
                    {msg.content}
                  </div>
                ))}
                {chatLoading && <div style={{ color: "#64748b", fontSize: 13 }}>Thinking...</div>}
              </div>
              <div style={{ display: "flex", gap: 8 }}>
                <input
                  type="text"
                  placeholder="e.g. I need a casual outfit for brunch..."
                  value={chatInput}
                  onChange={(e) => setChatInput(e.target.value)}
                  onKeyDown={(e) => e.key === "Enter" && sendChat()}
                  style={{ ...inputStyle, flex: 1 }}
                />
                <button onClick={sendChat} style={{ ...navBtn, background: "#3b82f6", padding: "8px 20px" }}>Send</button>
              </div>
            </div>
          </div>
        ) : null}
      </main>
    </div>
  );
}

function StatCard({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <div style={cardStyle}>
      <div style={{ fontSize: 12, color: "#94a3b8", marginBottom: 4 }}>{label}</div>
      <div style={{ fontSize: 24, fontWeight: 700, color: color || "#e2e8f0" }}>{value}</div>
    </div>
  );
}

function MiniStat({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <div>
      <div style={{ fontSize: 12, color: "#94a3b8" }}>{label}</div>
      <div style={{ fontSize: 16, fontWeight: 600, color: color || "#e2e8f0", marginTop: 2 }}>{value}</div>
    </div>
  );
}

const navBtn: React.CSSProperties = { border: "none", color: "#e2e8f0", padding: "6px 16px", borderRadius: 6, cursor: "pointer", fontSize: 14 };
const inputStyle: React.CSSProperties = { background: "#0f172a", border: "1px solid #334155", borderRadius: 8, padding: "10px 14px", color: "#e2e8f0", width: "100%", fontSize: 14, outline: "none" };
const cardStyle: React.CSSProperties = { background: "#1e293b", borderRadius: 12, padding: 24, border: "1px solid #334155" };
const cellStyle: React.CSSProperties = { padding: "10px 12px", fontSize: 14 };

export default App;
