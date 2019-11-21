using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using Userservice;

namespace Webapi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class UserController : ControllerBase
    {
        private readonly IKafkaSyncService _kafkaSyncService;

        public UserController(IKafkaSyncService kafkaSyncService)
        {
            _kafkaSyncService = kafkaSyncService;
        }

        [HttpGet("{id}")]
        public async Task<User> GetUserById(Guid id)
        { 
            var data = await _kafkaSyncService.Send("GetUser", id.ToString());

            return JsonConvert.DeserializeObject<User>(data);
        }
    }
}