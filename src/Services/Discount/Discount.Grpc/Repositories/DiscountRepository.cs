using Dapper;
using Discount.Grpc.Entities;
using Microsoft.Extensions.Configuration;
using Npgsql;
using System;
using System.Threading.Tasks;

namespace Discount.Grpc.Repositories {
    public class DiscountRepository : IDiscountRepository
    {
        private readonly IConfiguration _configuration;

        public DiscountRepository(IConfiguration configuration)
        {
            _configuration = configuration;
        }
        public async Task<Coupon> GetDiscount(string productName)
        {
            var connection = new NpgsqlConnection(
                _configuration.GetValue<string>("DatabaseSettings:ConnectionString"));
            var coupon = await connection.QueryFirstOrDefaultAsync<Coupon>(
                "SELECT * FROM Coupon WHERE ProductName = @ProductName",
                new {ProductName = productName});
            if (coupon == null)
            {
                return new Coupon()
                {
                    Amount = 0,
                    Description = "No Discount on this product",
                    ProductName = productName
                };
            }

            return coupon;
        }

        public async Task<bool> CreateDiscount(Coupon coupon)
        {
            var connection = new NpgsqlConnection(_configuration.GetValue<String>(
                "DatabaseSettings:ConnectionString"));
            var insertedItems = await connection.ExecuteAsync(
                "INSERT INTO Coupon (ProductName, Description, Amount)" +
                "VALUES (@ProductName, @Description, @Amount)",
                new {ProductName = coupon.ProductName, Description = coupon.Description, Amount = coupon.Amount});

            if (insertedItems == 0)
            {
                return false;
            }

            return true;
        }

        public async Task<bool> UpdateDiscount(Coupon coupon)
        {
            var connection = new NpgsqlConnection(_configuration.GetValue<String>(
                "DatabaseSettings:ConnectionString"));
            var updatedItems = await connection.ExecuteAsync(
                "UPDATE Coupon SET ProductName = @ProductName, Description = @Description, Amount = @Amount WHERE Id = @Id",
                new { Id = coupon.Id, ProductName = coupon.ProductName, Description = coupon.Description, Amount = coupon.Amount });

            if (updatedItems == 0) {
                return false;
            }

            return true;
        }

        public async Task<bool> DeleteDiscount(string productName) {
            var connection = new NpgsqlConnection(_configuration.GetValue<String>(
                "DatabaseSettings:ConnectionString"));
            var deletedItems = await connection.ExecuteAsync(
                "DELETE FROM Coupon WHERE ProductName = @ProductName",
                new { ProductName = productName});

            if (deletedItems == 0) {
                return false;
            }

            return true;
        }
    }
}
