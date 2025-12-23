(() => {
  const labels = window.__labels || [];
  const orderCounts = window.__orderCounts || [];
  const revenues = window.__revenues || [];

  const ordersCtx = document.getElementById("ordersChart");
  if (ordersCtx) {
    new Chart(ordersCtx, {
      type: "line",
      data: {
        labels,
        datasets: [{ label: "Orders", data: orderCounts }]
      },
      options: { responsive: true }
    });
  }

  const revenueCtx = document.getElementById("revenueChart");
  if (revenueCtx) {
    new Chart(revenueCtx, {
      type: "bar",
      data: {
        labels,
        datasets: [{ label: "Revenue (KRW)", data: revenues }]
      },
      options: { responsive: true }
    });
  }
})();
