document.addEventListener('DOMContentLoaded', () => {
    const frameworkSelect = document.getElementById('framework');
    const suppliersContainer = document.getElementById('suppliers-container');
    const detailsBox = document.getElementById('supplier-details');

    function formatCurrency(value) {
        return new Intl.NumberFormat('en-GB', { style: 'currency', currency: 'GBP' }).format(value);
    }

    function attachEventListenersToSupplierBoxes() {
        const supplierBoxes = document.querySelectorAll('.supplier-box');
        supplierBoxes.forEach(box => {
            box.addEventListener('click', () => {
                const details = JSON.parse(box.dataset.details);
                const name = box.dataset.name;
                let detailsHtml = `<h2>${name}</h2>`;
                const detailOrder = ['Buyer name', 'Contract value', 'Contract start', 'Contract end', 'Reported spend', 'Suggested email'];

                for (const key of detailOrder) {
                    if (details.hasOwnProperty(key)) {
                        const value = details[key];
                        const label = key === 'Suggested email' ? 'Email Address' : key;
                        let formattedValue = value;
                        if (['Contract value', 'Reported spend'].includes(key)) {
                            formattedValue = formatCurrency(value);
                        }
                        detailsHtml += `<p><strong>${label}:</strong> ${formattedValue}</p>`;
                    }
                }
                const buyerName = details['Buyer name'];
                const draftEmail = `Dear ${name},\n\nWe are writing to you regarding your agreement with ${buyerName}. Please can you check your records for any unreported spend.\n\nBest regards,\nCrown Commercial Service`;
                detailsHtml += `<p><strong>Draft Email:</strong></p><textarea readonly style="width: 100%; height: 150px; resize: vertical; background-color: #f5f5f7; border: 1px solid #d2d2d7; border-radius: 12px; padding: 10px; color: #1d1d1f;">${draftEmail}</textarea>`;
                detailsBox.innerHTML = detailsHtml;
                detailsBox.style.display = 'block';
            });
        });
    }

    frameworkSelect.addEventListener('change', () => {
        const selectedFramework = frameworkSelect.value;
        detailsBox.style.display = 'none';
        fetch(`/suppliers/${selectedFramework}`)
            .then(response => response.json())
            .then(suppliers => {
                suppliersContainer.innerHTML = '';
                suppliers.forEach(supplier => {
                    const supplierBox = document.createElement('div');
                    supplierBox.className = `supplier-box ${supplier.color}`;
                    supplierBox.dataset.details = JSON.stringify(supplier.details);
                    supplierBox.dataset.name = supplier.name;
                    supplierBox.textContent = supplier.name;
                    suppliersContainer.appendChild(supplierBox);
                });
                attachEventListenersToSupplierBoxes();
            });
    });

    attachEventListenersToSupplierBoxes();
});
